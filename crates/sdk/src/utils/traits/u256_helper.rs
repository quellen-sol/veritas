/// For primitive_types::U256 objs
pub trait StepU256Helper {
    fn shift_word_left(&self) -> Self;
    fn checked_shift_word_left(&self) -> Option<Self>
    where
        Self: Sized;
}

impl StepU256Helper for primitive_types::U256 {
    fn shift_word_left(&self) -> Self {
        let mut new_words = [0; 4];

        for i in (0..3).rev() {
            new_words[i + 1] = self.0[i];
        }

        Self(new_words)
    }

    fn checked_shift_word_left(&self) -> Option<Self> {
        let last = self.0.last();

        match last {
            None => Some(self.shift_word_left()),
            Some(element) => {
                if *element > 0 {
                    None
                } else {
                    Some(self.shift_word_left())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use primitive_types::U256;

    use super::StepU256Helper;

    #[test]
    fn shifting() {
        let u64_val = 69420u64;
        let num = U256([u64_val, 0, 0, 0]);
        let shifted_once = num.shift_word_left();
        let checked_shifted_twice = shifted_once.checked_shift_word_left().unwrap();

        assert_eq!(shifted_once.0, [0, u64_val, 0, 0]);
        assert_eq!(checked_shifted_twice.0, [0, 0, u64_val, 0]);
    }
}
