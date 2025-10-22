import { cn } from "@/lib/cn";
import type { PolymorphicProps } from "@kobalte/core/polymorphic";
import type {
  TextFieldDescriptionProps,
  TextFieldErrorMessageProps,
  TextFieldInputProps,
  TextFieldLabelProps,
  TextFieldRootProps,
} from "@kobalte/core/text-field";
import { TextField as TextFieldPrimitive } from "@kobalte/core/text-field";
import { cva } from "class-variance-authority";
import type { ValidComponent, VoidProps } from "solid-js";
import { splitProps } from "solid-js";

type TextFieldProps<T extends ValidComponent = "div"> =
  TextFieldRootProps<T> & {
    class?: string;
  };

export const TextFieldRoot = <T extends ValidComponent = "div">(
  props: PolymorphicProps<T, TextFieldProps<T>>,
) => {
  const [local, rest] = splitProps(props as TextFieldProps, ["class"]);

  return <TextFieldPrimitive class={cn("space-y-1", local.class)} {...rest} />;
};

export const textfieldLabel = cva(
  "text-sm data-[disabled]:cursor-not-allowed data-[disabled]:opacity-70 font-medium",
  {
    variants: {
      label: {
        true: "data-[invalid]:text-destructive",
      },
      error: {
        true: "text-destructive text-xs",
      },
      description: {
        true: "font-normal text-muted-foreground",
      },
    },
    defaultVariants: {
      label: true,
    },
  },
);

type TextFieldLabelPropsPlus<T extends ValidComponent = "label"> =
  TextFieldLabelProps<T> & {
    class?: string;
  };

export const TextFieldLabel = <T extends ValidComponent = "label">(
  props: PolymorphicProps<T, TextFieldLabelPropsPlus<T>>,
) => {
  const [local, rest] = splitProps(props as TextFieldLabelPropsPlus, ["class"]);

  return (
    <TextFieldPrimitive.Label
      class={cn(textfieldLabel(), local.class)}
      {...rest}
    />
  );
};

type TextFieldErrorMessagePropsPlus<T extends ValidComponent = "div"> =
  TextFieldErrorMessageProps<T> & {
    class?: string;
  };

export const TextFieldErrorMessage = <T extends ValidComponent = "div">(
  props: PolymorphicProps<T, TextFieldErrorMessagePropsPlus<T>>,
) => {
  const [local, rest] = splitProps(props as TextFieldErrorMessagePropsPlus, [
    "class",
  ]);

  return (
    <TextFieldPrimitive.ErrorMessage
      class={cn(textfieldLabel({ error: true }), local.class)}
      {...rest}
    />
  );
};

type TextFieldDescriptionPropsPlus<T extends ValidComponent = "div"> =
  TextFieldDescriptionProps<T> & {
    class?: string;
  };

export const TextFieldDescription = <T extends ValidComponent = "div">(
  props: PolymorphicProps<T, TextFieldDescriptionPropsPlus<T>>,
) => {
  const [local, rest] = splitProps(props as TextFieldDescriptionPropsPlus, [
    "class",
  ]);

  return (
    <TextFieldPrimitive.Description
      class={cn(
        textfieldLabel({ description: true, label: false }),
        local.class,
      )}
      {...rest}
    />
  );
};

type TextFieldInputPropsPlus<T extends ValidComponent = "input"> = VoidProps<
  TextFieldInputProps<T> & {
    class?: string;
  }
>;

export const TextField = <T extends ValidComponent = "input">(
  props: PolymorphicProps<T, TextFieldInputPropsPlus<T>>,
) => {
  const [local, rest] = splitProps(props as TextFieldInputPropsPlus, ["class"]);

  return (
    <TextFieldPrimitive.Input
      class={cn(
        "flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-shadow file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-[1.5px] focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50",
        local.class,
      )}
      {...rest}
    />
  );
};
