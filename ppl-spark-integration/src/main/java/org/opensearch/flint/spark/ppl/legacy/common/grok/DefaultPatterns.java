/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl.legacy.common.grok;

import java.util.Map;

public interface DefaultPatterns {

    /**
     * populate map with default patterns as they appear under the '/resources/patterns/*' resource folder 
     */
    static Map<String, String> withDefaultPatterns(Map<String, String> patterns) {
        patterns.put("PATH" , "(?:%{UNIXPATH}|%{WINPATH})");
        patterns.put("MONTH" , "\\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\\b");
        patterns.put("TZ" , "(?:[PMCE][SD]T|UTC)");
        patterns.put("DATESTAMP_OTHER" , "%{DAY} %{MONTH} %{MONTHDAY} %{TIME} %{TZ} %{YEAR}");
        patterns.put("HTTPDATE" , "%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}");
        patterns.put("HOST" , "%{HOSTNAME:UNWANTED}");
        patterns.put("DATESTAMP_EVENTLOG" , "%{YEAR}%{MONTHNUM2}%{MONTHDAY}%{HOUR}%{MINUTE}%{SECOND}");
        patterns.put("MESSAGESLOG" , "%{SYSLOGBASE} %{DATA}");
        patterns.put("WINDOWSMAC" , "(?:(?:[A-Fa-f0-9]{2}-){5}[A-Fa-f0-9]{2})");
        patterns.put("YEAR" , "(?>\\d\\d){1,2}");
        patterns.put("POSINT" , "\\b(?:[1-9][0-9]*)\\b");
        patterns.put("USERNAME" , "[a-zA-Z0-9._-]+");
        patterns.put("MINUTE" , "(?:[0-5][0-9])");
        patterns.put("UUID" , "[A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12}");
        patterns.put("DATE_US" , "%{MONTHNUM}[/-]%{MONTHDAY}[/-]%{YEAR}");
        patterns.put("LOGLEVEL" , "([A|a]lert|ALERT|[T|t]race|TRACE|[D|d]ebug|DEBUG|[N|n]otice|NOTICE|[I|i]nfo|INFO|[W|w]arn?(?:ing)?|WARN?(?:ING)?|[E|e]rr?(?:or)?|ERR?(?:OR)?|[C|c]rit?(?:ical)?|CRIT?(?:ICAL)?|[F|f]atal|FATAL|[S|s]evere|SEVERE|EMERG(?:ENCY)?|[Ee]merg(?:ency)?)");
        patterns.put("WINPATH" , "(?>[A-Za-z]+:|\\)(?:\\[^\\?*]*)+");
        patterns.put("NUMBER" , "(?:%{BASE10NUM:UNWANTED})");
        patterns.put("WORD" , "\\b\\w+\\b");
        patterns.put("QS" , "%{QUOTEDSTRING:UNWANTED}");
        patterns.put("TIMESTAMP_ISO8601" , "%{YEAR}-%{MONTHNUM}-%{MONTHDAY}[T ]%{HOUR}:?%{MINUTE}(?::?%{SECOND})?%{ISO8601_TIMEZONE}?");
        patterns.put("MONTHNUM" , "(?:0?[1-9]|1[0-2])");
        patterns.put("NOTSPACE" , "\\S+");
        patterns.put("IPV6" , "((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:)))(%.+)?");
        patterns.put("IPV4" , "(?<![0-9])(?:(?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2}))(?![0-9])");
        patterns.put("IP" , "(?:%{IPV6:UNWANTED}|%{IPV4:UNWANTED})");
        patterns.put("MAC" , "(?:%{CISCOMAC:UNWANTED}|%{WINDOWSMAC:UNWANTED}|%{COMMONMAC:UNWANTED})");
        patterns.put("DATE" , "%{DATE_US}|%{DATE_EU}");
        patterns.put("SYSLOGHOST" , "%{IPORHOST}");
        patterns.put("DATE_EU" , "%{MONTHDAY}[./-]%{MONTHNUM}[./-]%{YEAR}");
        patterns.put("DATA" , ".*?");
        patterns.put("SYSLOGTIMESTAMP" , "%{MONTH} +%{MONTHDAY} %{TIME}");
        patterns.put("URIPATHPARAM" , "%{URIPATH}(?:%{URIPARAM})?");
        patterns.put("CISCOMAC" , "(?:(?:[A-Fa-f0-9]{4}\\.){2}[A-Fa-f0-9]{4})");
        patterns.put("URIPARAM" , "\\?[A-Za-z0-9$.+!*'|(){},~@#%&/=:;_?\\-\\[\\]]*");
        patterns.put("MONTHDAY" , "(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])");
        patterns.put("DATESTAMP_RFC2822" , "%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{TIME} %{ISO8601_TIMEZONE}");
        patterns.put("COMMONAPACHELOG", "%{IPORHOST:clientip} %{USER:ident} %{USER:auth} \\[%{HTTPDATE:timestamp}\\] \"(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})\" %{NUMBER:response} (?:%{NUMBER:bytes}|-)");
        patterns.put("HOUR" , "(?:2[0123]|[01]?[0-9])");
        patterns.put("MONTHNUM2" , "(?:0[1-9]|1[0-2])");
        patterns.put("COMMONAPACHELOG_DATATYPED" , "%{IPORHOST:clientip} %{USER:ident;boolean} %{USER:auth} \\[%{HTTPDATE:timestamp;date;dd/MMM/yyyy:HH:mm:ss Z}\\] \"(?:%{WORD:verb;string} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion;float})?|%{DATA:rawrequest})\" %{NUMBER:response;int} (?:%{NUMBER:bytes;long}|-)");
        patterns.put("BASE10NUM" , "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
        patterns.put("NONNEGINT" , "\\b(?:[0-9]+)\\b");
        patterns.put("DATESTAMP_RFC822" , "%{DAY} %{MONTH} %{MONTHDAY} %{YEAR} %{TIME} %{TZ}");
        patterns.put("URI" , "%{URIPROTO}://(?:%{USER}(?::[^@]*)?@)?(?:%{URIHOST})?(?:%{URIPATHPARAM})?");
        patterns.put("INT" , "(?:[+-]?(?:[0-9]+))");
        patterns.put("SPACE" , "\\s*");
        patterns.put("GREEDYDATA" , ".*");
        patterns.put("ISO8601_SECOND" , "(?:%{SECOND}|60)");
        patterns.put("UNIXPATH" , "(?>/(?>[\\w_%!$@:.,~-]+|\\.)*)+");
        patterns.put("TTY" , "(?:/dev/(pts|tty([pq])?)(\\w+)?/?(?:[0-9]+))");
        patterns.put("COMBINEDAPACHELOG" , "%{COMMONAPACHELOG} %{QS:referrer} %{QS:agent}");
        patterns.put("URIPROTO" , "[A-Za-z]+(\\+[A-Za-z+]+)?");
        patterns.put("HOSTPORT" , "(?:%{IPORHOST}:%{POSINT:PORT})");
        patterns.put("SYSLOGPROG" , "%{PROG:program}(?:\\[%{POSINT:pid}\\])?");
        patterns.put("SYSLOGBASE" , "%{SYSLOGTIMESTAMP:timestamp} (?:%{SYSLOGFACILITY} )?%{SYSLOGHOST:logsource} %{SYSLOGPROG}:");
        patterns.put("SYSLOGFACILITY" , "<%{NONNEGINT:facility}.%{NONNEGINT:priority}>");
        patterns.put("DATESTAMP" , "%{DATE}[- ]%{TIME}");
        patterns.put("TIME" , "(?!<[0-9])%{HOUR}:%{MINUTE}(?::%{SECOND})(?![0-9])");
        patterns.put("USER" , "%{USERNAME:UNWANTED}");
        patterns.put("COMMONMAC" , "(?:(?:[A-Fa-f0-9]{2}:){5}[A-Fa-f0-9]{2})");
        patterns.put("IPORHOST" , "(?:%{HOSTNAME:UNWANTED}|%{IP:UNWANTED})");
        patterns.put("BASE16NUM" , "(?<![0-9A-Fa-f])(?:[+-]?(?:0x)?(?:[0-9A-Fa-f]+))");
        patterns.put("URIHOST" , "%{IPORHOST}(?::%{POSINT:port})?");
        patterns.put("BASE16FLOAT" , "\\b(?<![0-9A-Fa-f.])(?:[+-]?(?:0x)?(?:(?:[0-9A-Fa-f]+(?:\\.[0-9A-Fa-f]*)?)|(?:\\.[0-9A-Fa-f]+)))\\b");
        patterns.put("HOSTNAME" , "\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b)");
        patterns.put("URIPATH" , "(?:/[A-Za-z0-9$.+!*'(){},~:;=@#%_\\-]*)+");
        patterns.put("SECOND" , "(?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)");
        patterns.put("QUOTEDSTRING", "(?>(?<!\\\\)(?>\"(?>\\\\.|[^\\\\\"]+)+\"|\"\"|(?>'(?>\\\\.|[^\\\\']+)+')|''|(?>`(?>\\\\.|[^\\\\`]+)+`)|``))");
        patterns.put("DAY" , "(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)");
        patterns.put("ISO8601_TIMEZONE" , "(?:Z|[+-]%{HOUR}(?::?%{MINUTE}))");
        patterns.put("PROG" , "(?:[\\w._/%-]+)");    
        return patterns;
    }
}
