//! Test-only BSON deletion reduction; every accepted change is re-evaluated.
#![allow(dead_code)] // Shared by the full matrix and the diagnostic replay target.

use std::{
    error::Error,
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use briskdb::document::{
    BsonDocument, BsonValue, DocumentMatcher, DocumentMutationError, DocumentProjector,
    DocumentQueryError, DocumentUpdateError, DocumentUpdater, decode_document, encode_document,
};

pub const SOURCE_COMMIT: &str = "53cbf44e98b8caa036163725d195fd29592e1cc0";
const INPUT_LIMIT: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Match(bool),
    Document(Vec<u8>),
    Code(i32),
    NativeFailure(String),
}

#[derive(Clone, Copy)]
pub enum Surface {
    Matcher,
    Projection,
    Update,
}

// Keep the matcher entry points stable for existing regression/replay callers.
pub fn inputs(case: &BsonDocument) -> Result<(&BsonDocument, &BsonDocument), String> {
    Surface::Matcher.inputs(case)
}
pub fn expected(case: &BsonDocument) -> Result<Outcome, String> {
    Surface::Matcher.expected(case)
}
pub fn candidate(case: &BsonDocument) -> Outcome {
    Surface::Matcher.candidate(case)
}
pub fn reference(python: &str, case: &BsonDocument) -> Result<BsonDocument, String> {
    Surface::Matcher.reference(python, case)
}

impl Surface {
    pub const fn kind(self) -> &'static str {
        match self {
            Self::Matcher => "matcher",
            Self::Projection => "projection",
            Self::Update => "update",
        }
    }
    const fn field(self) -> &'static str {
        match self {
            Self::Matcher => "query",
            Self::Projection => "projection",
            Self::Update => "update",
        }
    }
    pub fn inputs(self, case: &BsonDocument) -> Result<(&BsonDocument, &BsonDocument), String> {
        match (case.get_first("document"), case.get_first(self.field())) {
            (Some(BsonValue::Document(document)), Some(BsonValue::Document(query))) => {
                if matches!(self, Self::Update)
                    && (document.get_first("_id").is_none()
                        || query.is_empty()
                        || query.iter().any(|(name, _)| !name.starts_with('$')))
                {
                    return Err(
                        "update diagnostic requires _id and a nonempty operator document".into(),
                    );
                }
                Ok((document, query))
            }
            _ => Err(format!(
                "{} case requires document and {} objects",
                self.kind(),
                self.field()
            )),
        }
    }

    pub fn expected(self, case: &BsonDocument) -> Result<Outcome, String> {
        let field = match self {
            Self::Matcher => "matches",
            Self::Projection | Self::Update => "result",
        };
        match (self, case.get_first(field), case.get_first("error")) {
            (Self::Matcher, Some(BsonValue::Boolean(value)), None) => Ok(Outcome::Match(*value)),
            (Self::Projection | Self::Update, Some(BsonValue::Document(value)), None) => {
                encode_document(value)
                    .map(Outcome::Document)
                    .map_err(|error| error.to_string())
            }
            (_, None, Some(BsonValue::Int32(code))) => Ok(Outcome::Code(*code)),
            _ => Err(format!(
                "{} case requires exactly one reference outcome",
                self.kind()
            )),
        }
    }

    pub fn candidate(self, case: &BsonDocument) -> Outcome {
        let (document, spec) = self.inputs(case).expect("validated differential inputs");
        let result = match self {
            Self::Matcher => DocumentMatcher::compile(spec)
                .and_then(|matcher| matcher.matches(document))
                .map(Outcome::Match),
            Self::Projection => DocumentProjector::compile(spec)
                .and_then(|projector| projector.project(document))
                .map(|result| {
                    Outcome::Document(encode_document(&result).expect("bounded projection output"))
                }),
            Self::Update => DocumentUpdater::compile(spec)
                .and_then(|updater| updater.apply(document))
                .map(|result| {
                    Outcome::Document(encode_document(&result).expect("bounded update output"))
                }),
        };
        match result {
            Ok(value) => value,
            Err(error) => {
                let code = error.source().and_then(|source| {
                    source
                        .downcast_ref::<DocumentUpdateError>()
                        .map(|error| error.mongo_code())
                        .or_else(|| {
                            source
                                .downcast_ref::<DocumentMutationError>()
                                .map(|error| error.mongo_code())
                        })
                        .or_else(|| {
                            source
                                .downcast_ref::<DocumentQueryError>()
                                .map(|error| error.mongo_code())
                        })
                });
                code.map_or_else(
                    || Outcome::NativeFailure(format!("{:?}", error.kind())),
                    Outcome::Code,
                )
            }
        }
    }

    pub fn reference(self, python: &str, case: &BsonDocument) -> Result<BsonDocument, String> {
        self.inputs(case)?;
        let payload = encode_document(case).map_err(|error| error.to_string())?;
        if payload.len() > INPUT_LIMIT {
            return Err("differential diagnostic input exceeds 64 KiB".into());
        }
        // Files avoid pipe-buffer deadlocks and let the parent enforce the worker
        // deadline even if the reference stalls before reading its input.
        let mut input = tempfile::tempfile().map_err(|error| error.to_string())?;
        input
            .write_all(&payload)
            .map_err(|error| error.to_string())?;
        input.rewind().map_err(|error| error.to_string())?;
        let mut output = tempfile::tempfile().map_err(|error| error.to_string())?;
        let mut child = Command::new(python)
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join(match self {
                Self::Matcher => "tests/document_matcher_oracle.py",
                Self::Projection => "tests/document_projection_oracle.py",
                Self::Update => "tests/document_update_oracle.py",
            }))
            .arg("--evaluate-one")
            .stdin(Stdio::from(input))
            .stdout(Stdio::from(
                output.try_clone().map_err(|error| error.to_string())?,
            ))
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| error.to_string())?;
        let started = Instant::now();
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => (),
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(error.to_string());
                }
            }
            if started.elapsed() >= Duration::from_secs(10) {
                let _ = child.kill();
                let _ = child.wait();
                return Err("source-locked differential diagnostic timed out".into());
            }
            thread::sleep(Duration::from_millis(5));
        };
        if !status.success() {
            return Err("source-locked differential diagnostic failed".into());
        }
        output
            .seek(SeekFrom::Start(0))
            .map_err(|error| error.to_string())?;
        let mut bytes = Vec::new();
        output
            .take((INPUT_LIMIT + 1025) as u64)
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        if bytes.len() > INPUT_LIMIT + 1024 {
            return Err("differential diagnostic output exceeded its bound".into());
        }
        let result = decode_document(&bytes).map_err(|error| error.to_string())?;
        self.inputs(&result)?;
        self.expected(&result)?;
        Ok(result)
    }

    pub fn reduce(
        self,
        case: &BsonDocument,
        max_probes: usize,
        max_time: Duration,
        preserves: impl FnMut(&BsonDocument) -> Result<bool, String>,
    ) -> Result<Reduction, String> {
        reduce_for(self, case, max_probes, max_time, preserves)
    }
    pub fn save_mismatch(
        self,
        python: &str,
        original: &BsonDocument,
        directory: &Path,
        candidate: impl Fn(&BsonDocument) -> Outcome,
    ) -> Result<PathBuf, String> {
        save_for(self, python, original, directory, candidate)
    }

    pub fn replay(self, python: &str, path: &Path) -> Result<(), String> {
        let mut bytes = Vec::new();
        fs::File::open(path)
            .map_err(|error| error.to_string())?
            .take(68 * 1024)
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        if bytes.len() >= 68 * 1024 {
            return Err("reproducer exceeds its bounded envelope".into());
        }
        let artifact = decode_document(&bytes).map_err(|error| error.to_string())?;
        if artifact.get_first("schema") != Some(&BsonValue::Int32(1))
            || artifact.get_first("kind") != Some(&BsonValue::from(self.kind()))
            || artifact.get_first("sourceCommit") != Some(&BsonValue::from(SOURCE_COMMIT))
        {
            return Err("reproducer metadata does not match this diagnostic".into());
        }
        let Some(BsonValue::Document(case)) = artifact.get_first("case") else {
            return Err("reproducer is missing its case".into());
        };
        let saved = self.expected(case)?;
        let refreshed = self.reference(python, case)?;
        let expected = self.expected(&refreshed)?;
        if saved != expected {
            return Err("saved reference outcome changed".into());
        }
        if self.candidate(&refreshed) != expected {
            return Err("saved mismatch still reproduces".into());
        }
        Ok(())
    }
}

enum Decision {
    Keep,
    Reject,
    Stop,
}
enum Search {
    Found(BsonValue),
    None,
    Stop,
}
type DeleteRange<'a> = Box<dyn Fn(std::ops::Range<usize>) -> BsonValue + 'a>;

fn document(values: Vec<(String, BsonValue)>) -> BsonValue {
    BsonValue::Document(BsonDocument::from_entries(values).expect("existing BSON names"))
}

fn first_deletion(value: &BsonValue, check: &mut dyn FnMut(&BsonValue) -> Decision) -> Search {
    let (length, rebuild): (usize, DeleteRange<'_>) = match value {
        BsonValue::Document(row) => (
            row.len(),
            Box::new(|removed| {
                document(
                    row.iter()
                        .enumerate()
                        .filter(|(i, _)| !removed.contains(i))
                        .map(|(_, (name, value))| (name.to_owned(), value.clone()))
                        .collect(),
                )
            }),
        ),
        BsonValue::Array(values) => (
            values.len(),
            Box::new(|removed| {
                BsonValue::Array(
                    values
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| !removed.contains(i))
                        .map(|(_, value)| value.clone())
                        .collect(),
                )
            }),
        ),
        _ => return Search::None,
    };
    let mut width = length;
    while width > 0 {
        for start in (0..length).step_by(width) {
            let reduced = rebuild(start..(start + width).min(length));
            match check(&reduced) {
                Decision::Keep => return Search::Found(reduced),
                Decision::Stop => return Search::Stop,
                Decision::Reject => (),
            }
        }
        width /= 2;
    }
    for index in 0..length {
        let (child, replace): (&BsonValue, Box<dyn Fn(BsonValue) -> BsonValue + '_>) = match value {
            BsonValue::Document(row) => (
                row.iter().nth(index).unwrap().1,
                Box::new(move |replacement| {
                    document(
                        row.iter()
                            .enumerate()
                            .map(|(i, (name, value))| {
                                (
                                    name.to_owned(),
                                    if i == index {
                                        replacement.clone()
                                    } else {
                                        value.clone()
                                    },
                                )
                            })
                            .collect(),
                    )
                }),
            ),
            BsonValue::Array(values) => (
                &values[index],
                Box::new(move |replacement| {
                    let mut result = values.clone();
                    result[index] = replacement;
                    BsonValue::Array(result)
                }),
            ),
            _ => unreachable!(),
        };
        match first_deletion(child, &mut |replacement| {
            check(&replace(replacement.clone()))
        }) {
            Search::Found(child) => return Search::Found(replace(child)),
            Search::Stop => return Search::Stop,
            Search::None => (),
        }
    }
    Search::None
}

pub struct Reduction {
    pub case: BsonDocument,
    pub probes: usize,
    pub exhausted: bool,
}

pub fn reduce(
    case: &BsonDocument,
    max_probes: usize,
    max_time: Duration,
    preserves: impl FnMut(&BsonDocument) -> Result<bool, String>,
) -> Result<Reduction, String> {
    Surface::Matcher.reduce(case, max_probes, max_time, preserves)
}

fn reduce_for(
    surface: Surface,
    case: &BsonDocument,
    max_probes: usize,
    max_time: Duration,
    mut preserves: impl FnMut(&BsonDocument) -> Result<bool, String>,
) -> Result<Reduction, String> {
    if max_probes == 0 {
        return Err("reduction requires a positive probe budget".into());
    }
    let (row, query) = surface.inputs(case)?;
    let mut value = document(vec![
        ("document".into(), BsonValue::Document(row.clone())),
        (surface.field().into(), BsonValue::Document(query.clone())),
    ]);
    let BsonValue::Document(initial) = &value else {
        unreachable!()
    };
    if encode_document(initial)
        .map_err(|error| error.to_string())?
        .len()
        > INPUT_LIMIT
    {
        return Err("differential diagnostic input exceeds 64 KiB".into());
    }
    let started = Instant::now();
    if !preserves(initial)? {
        return Err("input does not reproduce the requested mismatch".into());
    }
    let mut probes = 1;
    let mut error = None;
    let exhausted = loop {
        match first_deletion(&value, &mut |candidate| {
            if probes >= max_probes || started.elapsed() >= max_time {
                return Decision::Stop;
            }
            let BsonValue::Document(case) = candidate else {
                return Decision::Reject;
            };
            if surface.inputs(case).is_err() {
                return Decision::Reject;
            }
            probes += 1;
            match preserves(case) {
                Ok(true) => Decision::Keep,
                Ok(false) => Decision::Reject,
                Err(failure) => {
                    error = Some(failure);
                    Decision::Stop
                }
            }
        }) {
            Search::Found(reduced) => value = reduced,
            Search::None => break false,
            Search::Stop => break true,
        }
    };
    if let Some(error) = error {
        return Err(error);
    }
    let BsonValue::Document(case) = value else {
        unreachable!()
    };
    Ok(Reduction {
        case,
        probes,
        exhausted,
    })
}

pub fn save_mismatch(
    python: &str,
    original: &BsonDocument,
    directory: &Path,
    candidate: impl Fn(&BsonDocument) -> Outcome,
) -> Result<PathBuf, String> {
    Surface::Matcher.save_mismatch(python, original, directory, candidate)
}

fn save_for(
    surface: Surface,
    python: &str,
    original: &BsonDocument,
    directory: &Path,
    candidate: impl Fn(&BsonDocument) -> Outcome,
) -> Result<PathBuf, String> {
    let initial = surface.reference(python, original)?;
    let signature = (surface.expected(&initial)?, candidate(&initial));
    if signature.0 == signature.1 {
        return Err("input has no confirmed differential mismatch".into());
    }
    // Keep the confirmed original even if a later probe fails or becomes flaky.
    let original_path = persist(
        surface,
        &initial,
        directory,
        0,
        false,
        "original",
        &signature.1,
    )?;
    let failure = |error: String| {
        format!(
            "{error}; confirmed original retained at {}",
            original_path.display()
        )
    };
    let reduction = surface
        .reduce(&initial, 128, Duration::from_secs(20), |case| {
            let refreshed = surface.reference(python, case)?;
            Ok((surface.expected(&refreshed)?, candidate(&refreshed)) == signature)
        })
        .map_err(failure)?;
    let refreshed = surface
        .reference(python, &reduction.case)
        .map_err(failure)?;
    if (surface.expected(&refreshed)?, candidate(&refreshed)) != signature {
        return Err(failure(
            "reduced mismatch did not reproduce on final verification".into(),
        ));
    }
    persist(
        surface,
        &refreshed,
        directory,
        reduction.probes,
        reduction.exhausted,
        "reduced",
        &signature.1,
    )
    .map_err(failure)
}

fn persist(
    surface: Surface,
    case: &BsonDocument,
    directory: &Path,
    probes: usize,
    exhausted: bool,
    phase: &str,
    actual: &Outcome,
) -> Result<PathBuf, String> {
    let artifact = BsonDocument::from_entries([
        ("schema", BsonValue::Int32(1)),
        ("sourceCommit", BsonValue::from(SOURCE_COMMIT)),
        ("kind", BsonValue::from(surface.kind())),
        ("case", BsonValue::Document(case.clone())),
        ("probes", BsonValue::Int64(probes as i64)),
        ("budgetExhausted", BsonValue::Boolean(exhausted)),
        ("phase", BsonValue::from(phase)),
        (
            "originalCandidateOutcome",
            BsonValue::from(match actual {
                Outcome::Document(bytes) => format!(
                    "Document({} bytes, blake3={})",
                    bytes.len(),
                    blake3::hash(bytes)
                ),
                other => format!("{other:?}"),
            }),
        ),
    ])
    .map_err(|error| error.to_string())?;
    let bytes = encode_document(&artifact).map_err(|error| error.to_string())?;
    let path = directory.join(format!(
        "{}-{}.bson",
        surface.kind(),
        blake3::hash(&bytes).to_hex()
    ));
    fs::create_dir_all(directory).map_err(|error| error.to_string())?;
    let mut staged =
        tempfile::NamedTempFile::new_in(directory).map_err(|error| error.to_string())?;
    staged
        .write_all(&bytes)
        .map_err(|error| error.to_string())?;
    staged
        .as_file()
        .sync_all()
        .map_err(|error| error.to_string())?;
    match staged.persist_noclobber(&path) {
        Ok(_) => (),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::metadata(&path)
                .map_err(|error| error.to_string())?
                .len()
                != bytes.len() as u64
                || fs::read(&path).map_err(|error| error.to_string())? != bytes
            {
                return Err("existing reproducer does not match its content-addressed name".into());
            }
        }
        Err(error) => return Err(error.to_string()),
    }
    Ok(path)
}
