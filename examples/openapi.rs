fn main() -> Result<(), Box<dyn std::error::Error>> {
    let document = briskdb::api::openapi_v1();
    println!("{}", serde_json::to_string_pretty(&document)?);
    Ok(())
}
