use dbkv::DBKV;

#[cfg(target_os = "windows")]
const USAGE: &str = "
Usage:
    dbkv.exe <command> [options]
Options:
    -h, --help      Show this help message
Commands:
    dbkv.exe FILE get KEY     Get the value for KEY from the database in FILE
    dbkv.exe FILE insert KEY VALUE   Set the value for KEY to VALUE in the database in FILE
    dbkv.exe FILE delete KEY  Delete the entry for KEY from the database in FILE
    dbkv.exe FILE update KEY VALUE  Update the value for KEY to VALUE in the database in FILE
";

// if you want you can map the target_arch  (aarch64, arm, mips, power-pc, x86_64) too
// also target_family (unix, windows)
// target_env (gnu, msvc, musl,  "")
// target_pointer_width (32, 64, 16) // The size (in bits) of the target architecture’s pointer. Used for isize, usize, * const, and * mut types.
// target_has_atomic (8, 16, 32, 64, 128, ptr) //Integer sizes that have support for atomic operations. During atomic operations, the CPU takes responsibility for preventing race conditions with shared data at the expense of performance. The word atomic is used in the sense of indivisible.
#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
Usage:
    dbkv <command> [options]
Options:
    -h, --help      Show this help message
Commands:
    dbkv FILE get KEY     Get the value for KEY from the database in FILE
    dbkv FILE insert KEY VALUE   Set the value for KEY to VALUE in the database in FILE
    dbkv FILE delete KEY  Delete the entry for KEY from the database in FILE
    dbkv FILE update KEY VALUE  Update the value for KEY to VALUE in the database in FILE
";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let fname = args.get(1).expect(&USAGE);
    let action = args.get(2).expect(&USAGE).as_ref();
    let key = args.get(3).expect(&USAGE).as_ref();
    let maybe_value = args.get(4);

    let path = std::path::Path::new(&fname);

    let mut store = DBKV::open(path).expect("Failed to open database"); // opens the db file
    store.load().expect("Failed to load database"); // this will create a index in memory
    //
    //loads the offsets of any pre-existing data into an in-memory index.
    //The code uses two type aliases, ByteStr and ByteString:

    match action {
        "get" => {
            let value = match store.get(key).unwrap() {
                None => {
                    eprintln!("{:?} Key not found", key);
                    return;
                }
                Some(v) => println!("{:?}", v),
            };
        }
        "delete" => store.delete(key).unwrap(),
        "insert" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.insert(key, value).unwrap();
        }

        "update" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.update(key, value).unwrap();
        }
        _ => {
            eprintln!("{}", USAGE);
            return;
        }
    }
}
