package schema

// InitializeValidator creates a new validator with an empty format registry.
// Callers must manually register formats to break import cycles.
func InitializeValidator() *Validator {
	return NewValidator(NewFormatRegistry())
}

// InitializeValidatorWithFormats is deprecated and behaves same as InitializeValidator.
// Formats must be registered manually.
func InitializeValidatorWithFormats(enableProtobuf bool, enableYAML bool) *Validator {
	return InitializeValidator()
}
