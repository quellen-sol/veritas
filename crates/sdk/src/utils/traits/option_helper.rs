pub trait VeritasOptionHelper<T> {
    /// Replace the value if the callback returns true, otherwise set the value to the new value
    ///
    /// Returns if the value was replaced, and the old value, if there was one
    fn replace_if<F>(&mut self, new_value: T, cb: F) -> (bool, Option<T>)
    where
        F: FnOnce(&T) -> bool;
}

impl<T> VeritasOptionHelper<T> for Option<T> {
    fn replace_if<F>(&mut self, new_value: T, cb: F) -> (bool, Option<T>)
    where
        F: FnOnce(&T) -> bool,
    {
        match self {
            Some(old_value) => {
                if cb(old_value) {
                    (true, self.replace(new_value))
                } else {
                    (false, None)
                }
            }
            None => (true, self.replace(new_value)),
        }
    }
}
