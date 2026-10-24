use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // O código gRPC do LND v0.21.4-beta é gerado uma vez (precisa de `protoc`) e guardado em
    // src/gen, para que máquinas sem `protoc` (como o Raspberry Pi) compilem sem ele.
    let gen = Path::new("src/gen");
    if gen.join("lnrpc.rs").exists() && gen.join("routerrpc.rs").exists() {
        return Ok(());
    }
    std::fs::create_dir_all(gen)?;
    tonic_build::configure()
        .build_server(false)
        .out_dir(gen)
        .compile_protos(
            &["proto/lightning.proto", "proto/routerrpc/router.proto"],
            &["proto"],
        )?;
    Ok(())
}
