import type { SVGProps } from 'react'

type Props = SVGProps<SVGSVGElement>
const Icon = ({ children, ...props }: Props) => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}>
    {children}
  </svg>
)

export const GridIcon = (p: Props) => <Icon {...p}><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></Icon>
export const UploadIcon = (p: Props) => <Icon {...p}><path d="M12 16V4m0 0L7 9m5-5 5 5"/><path d="M4 15v4a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-4"/></Icon>
export const SearchIcon = (p: Props) => <Icon {...p}><circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/></Icon>
export const GraphIcon = (p: Props) => <Icon {...p}><circle cx="5" cy="12" r="2.5"/><circle cx="18" cy="6" r="2.5"/><circle cx="18" cy="18" r="2.5"/><path d="m7.3 10.9 8.4-3.8M7.3 13.1l8.4 3.8"/></Icon>
export const AlertIcon = (p: Props) => <Icon {...p}><path d="M12 3 2.7 20h18.6L12 3Z"/><path d="M12 9v4m0 3h.01"/></Icon>
export const DatabaseIcon = (p: Props) => <Icon {...p}><ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v7c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 12v7c0 1.7 3.6 3 8 3s8-1.3 8-3v-7"/></Icon>
export const ChevronIcon = (p: Props) => <Icon {...p}><path d="m9 18 6-6-6-6"/></Icon>
export const DownloadIcon = (p: Props) => <Icon {...p}><path d="M12 3v12m0 0 5-5m-5 5-5-5"/><path d="M4 19h16"/></Icon>
export const EquipmentIcon = (p: Props) => <Icon {...p}><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.9l.1.1-2.8 2.8-.1-.1a1.7 1.7 0 0 0-1.9-.3 1.7 1.7 0 0 0-1 1.6v.2h-4V21a1.7 1.7 0 0 0-1-1.6 1.7 1.7 0 0 0-1.9.3l-.1.1L4.2 17l.1-.1a1.7 1.7 0 0 0 .3-1.9A1.7 1.7 0 0 0 3 14H2.8v-4H3a1.7 1.7 0 0 0 1.6-1 1.7 1.7 0 0 0-.3-1.9L4.2 7 7 4.2l.1.1a1.7 1.7 0 0 0 1.9.3A1.7 1.7 0 0 0 10 3V2.8h4V3a1.7 1.7 0 0 0 1 1.6 1.7 1.7 0 0 0 1.9-.3l.1-.1L19.8 7l-.1.1a1.7 1.7 0 0 0-.3 1.9 1.7 1.7 0 0 0 1.6 1h.2v4H21a1.7 1.7 0 0 0-1.6 1Z"/></Icon>
export const CloseIcon = (p: Props) => <Icon {...p}><path d="m6 6 12 12M18 6 6 18"/></Icon>
export const CheckIcon = (p: Props) => <Icon {...p}><path d="m5 12 4 4L19 6"/></Icon>
export const TrashIcon = (p: Props) => <Icon {...p}><path d="M4 7h16M9 7V4h6v3m-9 0 1 14h10l1-14M10 11v6m4-6v6"/></Icon>
export const ChainIcon = (p: Props) => <Icon {...p}><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></Icon>
export const SparkleIcon = (p: Props) => <Icon {...p}><path d="M12 3v2m0 14v2M3 12h2m14 0h2m-3.22-6.78-1.42 1.42M6.64 17.36l-1.42 1.42m0-12.78 1.42 1.42m10.72 10.72 1.42 1.42"/><circle cx="12" cy="12" r="4"/></Icon>
