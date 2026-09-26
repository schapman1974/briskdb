// Test-only bounded subscriber shared by native and real-wire tracing tests.
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

pub type Fields = BTreeMap<String, String>;

#[derive(Default)]
pub struct State {
    pub events: Vec<Fields>,
    pub spans: Vec<Fields>,
    pub live: BTreeMap<u64, usize>,
    next_id: u64,
}

#[derive(Clone, Default)]
pub struct Capture(pub Arc<Mutex<State>>);

struct Visitor(Fields);
impl Visit for Visitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
}

impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "briskdb::mongo"
    }
    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut visitor = Visitor(Fields::new());
        attributes.record(&mut visitor);
        let mut state = self.0.lock().unwrap();
        assert!(state.spans.len() < 256);
        state.spans.push(visitor.0);
        state.next_id += 1;
        let id = state.next_id;
        state.live.insert(id, 1);
        Id::from_u64(id)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut visitor = Visitor(Fields::new());
        event.record(&mut visitor);
        let mut state = self.0.lock().unwrap();
        assert!(state.events.len() < 256);
        state.events.push(visitor.0);
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
    fn clone_span(&self, id: &Id) -> Id {
        *self.0.lock().unwrap().live.get_mut(&id.into_u64()).unwrap() += 1;
        id.clone()
    }
    fn try_close(&self, id: Id) -> bool {
        let mut state = self.0.lock().unwrap();
        let references = state.live.get_mut(&id.into_u64()).unwrap();
        *references -= 1;
        if *references == 0 {
            state.live.remove(&id.into_u64());
            true
        } else {
            false
        }
    }
}
