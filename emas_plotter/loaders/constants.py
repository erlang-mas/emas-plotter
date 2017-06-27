import re


PATH_REGEX = re.compile('\A.+\/(.+)\/(.+)\/(.+)\/(.+)\Z')
ENTRY_REGEX = re.compile('(.+)\s+(.+)\s+\[(.+)\]\s+(.+)\s+<MEASUREMENT-(\d+)>\s+<STEP-(\d+)>\s+\[(.+)\]')
METRIC_REGEX = re.compile('{(\w+),([\d\-\.]+)}')
