#[allow(unused_doc_comment)]
error_chain! {
    foreign_links {
        Io(::std::io::Error);
    }

    errors {
        ColumnNameAlreadyExists(column_name: String) {
            display("Cannot add column to catalog: column {} already exists", column_name)
        }
        ColumnIdAlreadyExists(id: usize) {
            display("Cannot add column to catalog: column with id {} already exists", id)
        }
    }
}
