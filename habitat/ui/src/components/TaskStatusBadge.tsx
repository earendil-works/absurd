interface TaskStatusBadgeProps {
  status: string;
}

export function TaskStatusBadge(props: TaskStatusBadgeProps) {
  const statusClasses = () => {
    switch (props.status.toLowerCase()) {
      case "pending":
        return "bg-gray-100 text-gray-800 border-gray-300";
      case "running":
        return "bg-blue-100 text-blue-800 border-blue-300";
      case "sleeping":
        return "bg-yellow-100 text-yellow-800 border-yellow-300";
      case "completed":
        return "bg-green-100 text-green-800 border-green-300";
      case "failed":
        return "bg-red-100 text-red-800 border-red-300";
      case "cancelled":
        return "bg-orange-100 text-orange-800 border-orange-300";
      default:
        return "bg-gray-100 text-gray-800 border-gray-300";
    }
  };

  return (
    <span
      class={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-medium ${statusClasses()}`}
    >
      {props.status}
    </span>
  );
}
