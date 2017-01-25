;;; --------------------------------------------------------------------------
;;; 
;;;     _/_/_/      _/_/_/    _/_/      _/_/_/  _/_/_/
;;;    _/    _/  _/    _/  _/_/_/_/  _/    _/  _/    _/
;;;    _/    _/  _/    _/  _/        _/    _/  _/    _/
;;;    _/_/_/      _/_/_/    _/_/_/    _/_/_/  _/    _/
;;;    _/
;;;    _/
;;; 
;;; MODULE: paean.lisp
;;; 
;;; @package oi-paean
;;; @author  Dennis Drown
;;; @date    20 Jan 2013
;;; 
;;; @copyright 2016-2017 Dennis Drown and Ostrich Ideas
;;; 
;;; A paean (pron.: /ˈpiːən/) is a song or lyric poem expressing triumph or thanksgiving.
;;; In classical antiquity, it is usually performed by a chorus, but some examples seem
;;; intended for an individual voice (monody). It comes from the Greek παιάν (also παιήων
;;; or παιών), "song of triumph, any solemn song or chant." "Paeon" was also the name of
;;; a divine physician and an epithet of Apollo.
;;;
;;; @see https://en.wikipedia.org/wiki/Paean
;;; --------------------------------------------------------------------------
(require "asdf")
(asdf:load-system :ostrich-ideas)
(asdf:load-system :cl-fad)
(asdf:load-system :cl-who)
(asdf:load-system :postmodern)
(asdf:load-system :simple-date)
(asdf:load-system :simple-date-postgres-glue)
(asdf:load-system :split-sequence)

(rename-package :cl-fad         :cl-fad         '(:fad))
(rename-package :postmodern     :postmodern     '(:ssql))
(rename-package :simple-date    :simple-date    '(:dts))
(rename-package :split-sequence :split-sequence '(:split))
(rename-package :sb-posix       :sb-posix       '(:sys))

(unless (member :title cl-who:*HTML-NO-INDENT-TAGS*) (push :title cl-who:*HTML-NO-INDENT-TAGS*))
(unless (member :h1    cl-who:*HTML-NO-INDENT-TAGS*) (push :h1    cl-who:*HTML-NO-INDENT-TAGS*))
(unless (member :h2    cl-who:*HTML-NO-INDENT-TAGS*) (push :h2    cl-who:*HTML-NO-INDENT-TAGS*))
(unless (member :h3    cl-who:*HTML-NO-INDENT-TAGS*) (push :h3    cl-who:*HTML-NO-INDENT-TAGS*))
(unless (member :h4    cl-who:*HTML-NO-INDENT-TAGS*) (push :h4    cl-who:*HTML-NO-INDENT-TAGS*))
(unless (member :tr    cl-who:*HTML-NO-INDENT-TAGS*) (push :tr    cl-who:*HTML-NO-INDENT-TAGS*))
(unless (member :th    cl-who:*HTML-NO-INDENT-TAGS*) (push :th    cl-who:*HTML-NO-INDENT-TAGS*))
(unless (member :td    cl-who:*HTML-NO-INDENT-TAGS*) (push :td    cl-who:*HTML-NO-INDENT-TAGS*))


;; ---------------------------------------------------------------------------
(proclaim '(optimize (speed 0)
                     (debug 3)
                     (space 0)
                     (compilation-speed 0)))

(defpackage :oi-paean
  (:use   :common-lisp
          :ostrich-ideas
          :cl-who
          :split-sequence)
  (:nicknames :paean)
  (:export :sing
           :resing
           :chorus))

(in-package :oi-paean)


(defparameter +VERSION+        "0.2.1.0"    "System Version")

(defconstant +SECS-IN-MIN+          60)
(defconstant +SECS-IN-HOUR+         3600)
(defconstant +SECS-IN-DAY+          86400)
(defconstant +MIN-PROPHECY-DAYS+    20      "Minimum number of days for a full prophecy model")
(defconstant +KNOWN-DATA-DAYS+      90      "Number of days of past (known) data to show in results")

; FIXME: These values were hardcoded for apollo on Rocks and now for apollo on gentoo.
;        How are we going to marry this to the Sibyl project?
(defparameter *SIBYL-TARGET*        "close"                         "Target attribute for Sibyl processing")
(defparameter *PROPHECY-CWD*        #p"./*.p"                       "Location of Sibyl 'prophecy' output files")
(defparameter *PROPHECY-PATH*       #p"/opt/sibyl/com/prophecy/*.p" "Location of Sibyl 'prophecy' output files")
(defparameter *WEB-PATH*            #p"/opt/sibyl/com/www/*"        "Location of Sibyl web content")

(defparameter *KNOWN-DATA-INTERVAL* (dts:encode-interval :day +KNOWN-DATA-DAYS+)
                                    "Pre-compute simple-date interval object for past data")

(defparameter *MONTHS*              (make-array 13
                                                :element-type     'simple-string
                                                :initial-contents '("?M?"
                                                                    "Jan" "Feb" "Mar" "Apr"
                                                                    "May" "Jun" "Jul" "Aug"
                                                                    "Sep" "Oct" "Nov" "Dec")))

(defparameter *NYSE-HOLIDAYS*
    ; New-Years   MLK-Day   Washington  Good-Fri   Memorial   4th-July  Labor-Day  ThanksDay  Christmas
    ; ---------- ---------- ---------- ---------- ---------- ---------- ---------- ---------- ----------
    '("20160101" "20160118" "20160215" "20160325" "20160530" "20160704" "20160905" "20161124" "20161226"  ;2016
      "20170102" "20170116" "20170220" "20170414" "20170529" "20170704" "20170904" "20171123" "20171225") ;2017
    "
    Days the New York Stock Exchange is closed
    Source: http://www.profitconfidential.com/nyse-holidays-schedule-calendar/")


;; ---------------------------------------------------------------------------
;; ┏━┓┏━┓┏━┓┏━┓╻ ╻┏━╸┏━╸╻ ╻
;; ┣━┛┣┳┛┃ ┃┣━┛┣━┫┣╸ ┃  ┗┳┛
;; ╹  ╹┗╸┗━┛╹  ╹ ╹┗━╸┗━╸ ╹
;; ---------------------------------------------------------------------------
(defstruct prophecy
  (path       #p"" :type pathname)
  (symbol     ""   :type simple-string)
  (days       0    :type fixnum)
  (trade-date ""   :type simple-string))

(defparameter *NULL-PROPHECY* (make-prophecy))

;; ---------------------------------------------------------------------------
(ssql:defprepared sql-known-prices 
    (:order-by
      (:select (:to-char 'trade_date "YYYYMMDD") 'close
               :from 'prices
               :where (:AND (:=  'symbol     '$1)
                            (:>= 'trade_date '$2)
                            (:<= 'trade_date '$3)))
      'trade_date))

;; ---------------------------------------------------------------------------
(ssql:defprepared sql-now-known-prices 
    (:order-by
      (:select (:to-char 'trade_date "YYYYMMDD") 'close
               :from 'prices
               :where (:AND (:= 'symbol     '$1)
                            (:> 'trade_date '$2)))
      'trade_date))


;; ---------------------------------------------------------------------------
;; ┏━╸╺┳┓
;; ┃   ┃┃
;; ┗━╸╺┻┛
(declaim (inline cd))
;; ---------------------------------------------------------------------------
(defun cd (dir)
"
Changes to the directory specified  by DIR, regardless of whether DIR is a
lispy pathname or a string as required by sbcl.
 "
  (sb-posix:chdir (if (typep dir 'pathname)
                      (directory-namestring dir)
                      dir)))


;; ---------------------------------------------------------------------------
;; ┏━╸┏━┓┏━┓┏┳┓┏━┓╺┳╸   ╺┳┓┏━┓╺┳╸┏━╸
;; ┣╸ ┃ ┃┣┳┛┃┃┃┣━┫ ┃ ╺━╸ ┃┃┣━┫ ┃ ┣╸
;; ╹  ┗━┛╹┗╸╹ ╹╹ ╹ ╹    ╺┻┛╹ ╹ ╹ ┗━╸
;; ---------------------------------------------------------------------------
(defun format-date (fmt date)
 "
 Formats a date according to the specified format string
 "
   (multiple-value-bind (yyyy mm dd) (dts:decode-date date)
     (format nil fmt  yyyy mm dd)))


;; ---------------------------------------------------------------------------
;; ╺┳┓┏━┓╺┳╸┏━╸   ╺┳╸┏━┓   ╻ ╻╻ ╻╻ ╻╻ ╻   ┏┳┓┏┳┓   ╺┳┓╺┳┓
;;  ┃┃┣━┫ ┃ ┣╸ ╺━╸ ┃ ┃ ┃╺━╸┗┳┛┗┳┛┗┳┛┗┳┛╺━╸┃┃┃┃┃┃╺━╸ ┃┃ ┃┃
;; ╺┻┛╹ ╹ ╹ ┗━╸    ╹ ┗━┛    ╹  ╹  ╹  ╹    ╹ ╹╹ ╹   ╺┻┛╺┻┛
;; ---------------------------------------------------------------------------
(defmacro date-to-yyyy-mm-dd (date)
  `(format-date "~d-~2,'0d-~2,'0d" ,date))


;; ---------------------------------------------------------------------------
;; ╺┳┓┏━┓╺┳╸┏━╸   ╺┳╸┏━┓   ╻ ╻╻ ╻╻ ╻╻ ╻┏┳┓┏┳┓╺┳┓╺┳┓
;;  ┃┃┣━┫ ┃ ┣╸ ╺━╸ ┃ ┃ ┃╺━╸┗┳┛┗┳┛┗┳┛┗┳┛┃┃┃┃┃┃ ┃┃ ┃┃
;; ╺┻┛╹ ╹ ╹ ┗━╸    ╹ ┗━┛    ╹  ╹  ╹  ╹ ╹ ╹╹ ╹╺┻┛╺┻┛
;; ---------------------------------------------------------------------------
(defmacro date-to-yyyymmdd (date)
  `(format-date "~d~2,'0d~2,'0d" ,date))


;; ---------------------------------------------------------------------------
;; ╻ ╻╻ ╻╻ ╻╻ ╻┏┳┓┏┳┓╺┳┓╺┳┓   ╺┳╸┏━┓   ╺┳┓┏━┓╺┳╸┏━╸
;; ┗┳┛┗┳┛┗┳┛┗┳┛┃┃┃┃┃┃ ┃┃ ┃┃╺━╸ ┃ ┃ ┃╺━╸ ┃┃┣━┫ ┃ ┣╸
;;  ╹  ╹  ╹  ╹ ╹ ╹╹ ╹╺┻┛╺┻┛    ╹ ┗━┛   ╺┻┛╹ ╹ ╹ ┗━╸
;; ---------------------------------------------------------------------------
(defun yyyymmdd-to-date (yyyymmdd)
"
Returns a simple-date object which represents the compact date string
"
  (let ((dateTxt (the string yyyymmdd)))
    (dts:encode-date (parse-integer (subseq dateTxt 0 4))  ; yyyy
                     (parse-integer (subseq dateTxt 4 6))  ; mm
                     (parse-integer (subseq dateTxt 6))))) ; dd


;; ---------------------------------------------------------------------------
;; ╻ ╻╻ ╻╻ ╻╻ ╻┏┳┓┏┳┓╺┳┓╺┳┓   ╺┳╸┏━┓   ╺┳┓┏━┓╺┳╸┏━╸   ╺┳╸┏━╸╻ ╻╺┳╸
;; ┗┳┛┗┳┛┗┳┛┗┳┛┃┃┃┃┃┃ ┃┃ ┃┃╺━╸ ┃ ┃ ┃╺━╸ ┃┃┣━┫ ┃ ┣╸ ╺━╸ ┃ ┣╸ ┏╋┛ ┃
;;  ╹  ╹  ╹  ╹ ╹ ╹╹ ╹╺┻┛╺┻┛    ╹ ┗━┛   ╺┻┛╹ ╹ ╹ ┗━╸    ╹ ┗━╸╹ ╹ ╹
;; ---------------------------------------------------------------------------
(defmacro yyyymmdd-to-date-text (yyyymmdd)
"Returns date text in the form dd MON yyyy"

  `(let ((date-ymd (the string ,yyyymmdd)))
     (format nil "~a ~a ~a" (subseq date-ymd 6)                                    ; dd
                            (aref *MONTHS* (parse-integer (subseq date-ymd 4 6)))  ; "mon"
                            (subseq date-ymd 0 4))))                               ; yyyy
                      


;; ---------------------------------------------------------------------------
;; ╻ ╻╻ ╻╻ ╻╻ ╻┏┳┓┏┳┓╺┳┓╺┳┓   ╺┳╸┏━┓   ╻ ╻╻ ╻╻ ╻╻ ╻   ┏┳┓┏┳┓   ╺┳┓╺┳┓
;; ┗┳┛┗┳┛┗┳┛┗┳┛┃┃┃┃┃┃ ┃┃ ┃┃╺━╸ ┃ ┃ ┃╺━╸┗┳┛┗┳┛┗┳┛┗┳┛╺━╸┃┃┃┃┃┃╺━╸ ┃┃ ┃┃
;;  ╹  ╹  ╹  ╹ ╹ ╹╹ ╹╺┻┛╺┻┛    ╹ ┗━┛    ╹  ╹  ╹  ╹    ╹ ╹╹ ╹   ╺┻┛╺┻┛
;; ---------------------------------------------------------------------------
(defmacro yyyymmdd-to-yyyy-mm-dd (yyyymmdd)
"Returns date text in the form yyyy-mm-dd"

  `(let ((date-ymd ,yyyymmdd))
     (format nil "~a-~a-~a" (subseq date-ymd 0 4)       ; yyyy
                            (subseq date-ymd 4 6)       ; "mon"
                            (subseq date-ymd 6))))      ; dd
                            
                            
                      
;; ---------------------------------------------------------------------------
;; ╻ ╻╻ ╻╻ ╻╻ ╻┏┳┓┏┳┓╺┳┓╺┳┓   ╺┳╸┏━┓   ┏┳┓┏┳┓┏┳┓
;; ┗┳┛┗┳┛┗┳┛┗┳┛┃┃┃┃┃┃ ┃┃ ┃┃╺━╸ ┃ ┃ ┃╺━╸┃┃┃┃┃┃┃┃┃
;;  ╹  ╹  ╹  ╹ ╹ ╹╹ ╹╺┻┛╺┻┛    ╹ ┗━┛   ╹ ╹╹ ╹╹ ╹
;; ---------------------------------------------------------------------------
(defmacro yyyymmdd-to-mmm (yyyymmdd)
"Returns a simple-date object which represents the compact date string"

  `(aref *MONTHS* (parse-integer (subseq ,yyyymmdd 4 6))))


;; ---------------------------------------------------------------------------
;; ╻ ╻╻ ╻╻ ╻╻ ╻┏┳┓┏┳┓╺┳┓╺┳┓   ╺┳╸┏━┓   ┏━╸┏━┓┏━┓┏━┓╻ ╻   ┏━╸┏━┓╺┳┓┏━╸
;; ┗┳┛┗┳┛┗┳┛┗┳┛┃┃┃┃┃┃ ┃┃ ┃┃╺━╸ ┃ ┃ ┃╺━╸┃╺┓┣┳┛┣━┫┣━┛┣━┫╺━╸┃  ┃ ┃ ┃┃┣╸
;;  ╹  ╹  ╹  ╹ ╹ ╹╹ ╹╺┻┛╺┻┛    ╹ ┗━┛   ┗━┛╹┗╸╹ ╹╹  ╹ ╹   ┗━╸┗━┛╺┻┛┗━╸
;; ---------------------------------------------------------------------------
(defun yyyymmdd-to-graph-code (yyyymmdd)
"
Returns the day of the month (no prepended zero), unless the date represents
the first weekday (Mon --Fri) of the month, in which case it returns the
three-letter month code (Jan -- Dec).
"
  (let ((day (parse-integer (subseq yyyymmdd 6))))

    ; What we send back depends on the day of the month...
     (cond
       ; First of the month always gives MMM
       ((eql day 1) (yyyymmdd-to-mmm yyyymmdd))

       ; Second/third of the month gives MMM only on a Monday
       ((<= day 3) 
         (multiple-value-bind (sec min hr dom mon yr dow)
           (decode-universal-time (encode-universal-time 0 0 12 ; high noon
                                                         day
                                                         (parse-integer (subseq yyyymmdd 4 6))   ; month
                                                         (parse-integer (subseq yyyymmdd 0 4)))) ; year
           (declare (ignore sec min hr dom yr))

           ; Monday is the zeroth day of the week!
           (if (zerop dow)
               (aref *MONTHS* mon)      ; Monday gives MMM
               (princ-to-string day)))) ; Anything else gives plain day
           

     (t (princ-to-string day)))))       ; plain day (strip leading zero)


;; ---------------------------------------------------------------------------
;; ┏━┓┏━┓┏━┓╺┳╸╻╺┳╸╻┏━┓┏┓╻   ┏━┓┏━┓┏━┓┏━┓╻ ╻┏━╸┏━╸╻┏━╸┏━┓
;; ┣━┛┣━┫┣┳┛ ┃ ┃ ┃ ┃┃ ┃┃┗┫╺━╸┣━┛┣┳┛┃ ┃┣━┛┣━┫┣╸ ┃  ┃┣╸ ┗━┓
;; ╹  ╹ ╹╹┗╸ ╹ ╹ ╹ ╹┗━┛╹ ╹   ╹  ╹┗╸┗━┛╹  ╹ ╹┗━╸┗━╸╹┗━╸┗━┛
;; ---------------------------------------------------------------------------
(defun partition-prophecies (prophs)
 "
 Takes a list of prophecies and returns four things
    [1] The security and date tag as a string such as «AAPL/20160417»
    [2] The count of the prophecies in the primary partition [3]
    [3] A list with the first prophecy and all the prophecies in the same family
    [4] A list with all the remaining prophecies
 "
  (declare (type list prophs))

  (if (not (endp prophs))
    (let* ((model      (car prophs)) 
           (candidates (cdr prophs))
           (symbol     (prophecy-symbol     model))
           (trade-date (prophecy-trade-date model))
           (model-tag  (format nil "~a/~a" symbol trade-date)))

     (log-info "Checking ~a prophecies" model-tag)
      (loop for item in candidates
        if (and (equal symbol (prophecy-symbol item))
                (equal trade-date (prophecy-trade-date item)))
          sum 1 into cnt and
          collect item into hits
        else
          collect item into misses
        finally (return (values model-tag                   ;; Logging tag
                                (1+ cnt)                    ;; Adjust for zero based
                                (cons model hits)           ;; Don't forget the head of prophs
                                misses))))                  ;; All proph members that didn't match
    ; Nothing to process
    (values '!INCOMPLETE! 0 '() '())))


;; ---------------------------------------------------------------------------
;; ┏━╸╻ ╻╺┳╸┏━┓┏━┓┏━╸╺┳╸   ┏━┓┏━┓┏━┓┏━┓╻ ╻┏━╸┏━╸╻┏━╸┏━┓
;; ┣╸ ┏╋┛ ┃ ┣┳┛┣━┫┃   ┃ ╺━╸┣━┛┣┳┛┃ ┃┣━┛┣━┫┣╸ ┃  ┃┣╸ ┗━┓
;; ┗━╸╹ ╹ ╹ ╹┗╸╹ ╹┗━╸ ╹    ╹  ╹┗╸┗━┛╹  ╹ ╹┗━╸┗━╸╹┗━╸┗━┛
;; ---------------------------------------------------------------------------
(defun extract-prophecies (prophs)
"
Removes all prophecies belonging to the group described by the first prophecy
in the list
"
  (declare (type list prophs))

  (multiple-value-bind (family-tag
                        family-cnt
                        family-lst
                        others)     (partition-prophecies prophs)

   ; Same number of prophecies as the highest prophesied day?
   (if (and (>= family-cnt +MIN-PROPHECY-DAYS+)
            (=  family-cnt (reduce #'max (mapcar #'prophecy-days family-lst))))

     ; Create an array family from the list family
     (let ((ndx     0)
           (family  (make-array family-cnt :initial-element nil)))  ; *NULL-PROPHECY*
                                                                    ; :element-type 'prophecy))
       (dolist (proph family-lst)
         (setq ndx (1- (prophecy-days proph)))
         (if (aref family ndx)
             (log-warn "Duplicate prophecy match: '~a' AND '~a'" (prophecy-path (aref family ndx))
                                                                 (prophecy-path proph))
             (setf (aref family ndx) proph)))

       (log-info "Complete! Gathered ~d prophecies for ~a" family-cnt family-tag)
       (list family family-cnt others))

       ; Not enough to do this family
       (progn
         (unless (zerop family-cnt )
           (log-warn "Prophecy series incomplete: fam[~a] cnt[~d]" family-tag family-cnt))
         (list '!INCOMPLETE! 0 others)))))
    

;; ---------------------------------------------------------------------------
;; ┏━┓┏━┓╺┳╸╻ ╻   ╺┳╸┏━┓   ┏━┓┏━┓┏━┓┏━┓╻ ╻┏━╸┏━╸╻ ╻
;; ┣━┛┣━┫ ┃ ┣━┫╺━╸ ┃ ┃ ┃╺━╸┣━┛┣┳┛┃ ┃┣━┛┣━┫┣╸ ┃  ┗┳┛
;; ╹  ╹ ╹ ╹ ╹ ╹    ╹ ┗━┛   ╹  ╹┗╸┗━┛╹  ╹ ╹┗━╸┗━╸ ╹
;; ---------------------------------------------------------------------------
(defun path-to-prophecy (path)
"
Returns a prophecy structure based on all the goodness contained in a prophecy
run file name (Sibyl output)

    Example call
        (path-to-prophecy #P«/zoo/dog/opt/sibyl/com/prophecy/sibyl.GRMN.1.20160415.p»)

    Returned output:
        #S(PROPHECY
           :PATH #P«/zoo/dog/opt/sibyl/com/prophecy/sibyl.GRMN.1.20160415.p»
           :SYMBOL «GRMN»
           :DAYS 1
           :TRADE-DATE "20160415")
"
  (let* ((name  (file-namestring path))
         (parts (split-sequence #\. name)))
    (when (eql 5 (length parts))
      (make-prophecy :path       path
                     :symbol     (nth 1 parts)
                     :days       (parse-integer (nth 2 parts))
                     :trade-date (nth 3 parts)))))



;; ---------------------------------------------------------------------------
;; ┏━╸┏━┓┏━╸┏━┓╺┳╸┏━╸   ╺┳┓┏━┓╺┳╸┏━╸   ┏━┓┏━┓╻┏━╸┏━╸   ╺┳╸┏━┓┏┓ ╻  ┏━╸
;; ┃  ┣┳┛┣╸ ┣━┫ ┃ ┣╸ ╺━╸ ┃┃┣━┫ ┃ ┣╸ ╺━╸┣━┛┣┳┛┃┃  ┣╸ ╺━╸ ┃ ┣━┫┣┻┓┃  ┣╸
;; ┗━╸╹┗╸┗━╸╹ ╹ ╹ ┗━╸   ╺┻┛╹ ╹ ╹ ┗━╸   ╹  ╹┗╸╹┗━╸┗━╸    ╹ ╹ ╹┗━┛┗━╸┗━╸
;; ---------------------------------------------------------------------------
(defun create-date-price-table  (symbol trade-yyyymmdd known-data proph-data)
"Creates a trade date/price lookup table in HTML and returns it as a string"
  
  ;(log-debug "Prophecy data: ~a" proph-data)
  (let* ((data       (append known-data proph-data))
         (data-cnt   (length data))
         (cols       7)
         (last-col   (1- cols))
         (rows       (ceiling data-cnt cols)))

    (with-html-output-to-string (s nil :indent t)
      (:table :id "checker" :border "4"
        (:thead (:tr 
          (dotimes (col cols nil)           
            (htm (:th "Trade Date") (:th "Close"))
            (unless (eql col last-col) (htm (:th "&nbsp"))))))
      (:tbody
       (dotimes (row rows nil)
         (htm (:tr
           (dotimes (col cols nil)
             (let* ((data-ndx   (+ (* col rows) row))
                    (data-pair  (if (< data-ndx data-cnt)
                                 (nth data-ndx data)
                                 '("&nbsp;" "&nbsp;")))  
                    (trade-date  (car data-pair))
                    (proph-clue  (char trade-date 0))         ; First char of date designates KNOWN or PROPHESY
                    (prophesy?   (or (eql #\+ proph-clue)
                                     (eql #\& proph-clue))))

               (htm (:td :align "right" (if prophesy?
                                            (htm (:a :href (format nil "run.~a.close.~a.~a.html" ; Link to run
                                                                       symbol
                                                                       (parse-integer (subseq trade-date 1)
                                                                                      :junk-allowed t)
                                                                       trade-yyyymmdd)
                                                     (str trade-date)))
                                            (str (yyyymmdd-to-yyyy-mm-dd trade-date))))     ; Plain ole trade date
                    (:td :align "right"  (fmt "~$" (cadr data-pair))))                      ; Closing price
               
               ; Put in a spacer column, but not AFTER the data!
               (unless (eql col last-col)
                 (when (eql row 0) (htm (:td :rowspan rows "&nbsp"))))))))))))))


;; ---------------------------------------------------------------------------
;; ┏━╸┏━┓┏━┓┏┳┓┏━┓╺┳╸   ┏━┓┏━┓┏━┓┏━┓╻ ╻┏━╸┏━╸╻ ╻   ╺┳┓┏━┓╺┳╸┏━╸
;; ┣╸ ┃ ┃┣┳┛┃┃┃┣━┫ ┃ ╺━╸┣━┛┣┳┛┃ ┃┣━┛┣━┫┣╸ ┃  ┗┳┛╺━╸ ┃┃┣━┫ ┃ ┣╸
;; ╹  ┗━┛╹┗╸╹ ╹╹ ╹ ╹    ╹  ╹┗╸┗━┛╹  ╹ ╹┗━╸┗━╸ ╹    ╺┻┛╹ ╹ ╹ ┗━╸
;; ---------------------------------------------------------------------------
(let ((last-trade-date)
      (one-day          (dts:encode-interval :day 1))
      (nyse-holidays    (mapcar #'yyyymmdd-to-date *NYSE-HOLIDAYS*)))

  ;; -------------------------------------------------------------------------
  (defun set-last-trade-yyyymmdd (date)
  "
  Sets the value of (what will be) the last trade date we've considered.
  "
    (setq last-trade-date (yyyymmdd-to-date date)))

  ;; -------------------------------------------------------------------------
  (defun invalid-trade-date-p (date)
    (let ((dow      (dts:day-of-week date))
          (holiday? (member date nyse-holidays :test #'dts:time=)))
      (when holiday?
        (log-notice "NYSE is closed on ~a" (date-to-yyyy-mm-dd date)))
      (or (eql dow 0)   ; Sunday
          (eql dow 6)   ; Saturday
          holiday?)))   ; Party day

  ;; -------------------------------------------------------------------------
  (defun format-prophecy-date (proph-days known-date)
  "
  Provides the formatting for a prophecy value in a table cell, where the actual
  value may or may not be known.
  "
  (loop
    do (setq last-trade-date (dts:time-add last-trade-date one-day))
    while (invalid-trade-date-p last-trade-date))
  (let ((yyyy-mm-dd  (cond 
                       ((null known-date) (date-to-yyyy-mm-dd last-trade-date))
                       ((string= known-date
                                 (date-to-yyyymmdd last-trade-date)) (yyyymmdd-to-yyyy-mm-dd known-date))
                       (t "DATE-MISMATCH"))))
  (format nil "+~a : ~a" proph-days yyyy-mm-dd))))


;; ---------------------------------------------------------------------------
;; ┏━╸┏━┓┏━┓┏┳┓┏━┓╺┳╸   ┏━┓┏━┓┏━┓┏━┓╻ ╻┏━╸┏━╸╻ ╻   ┏━╸┏━╸╻  ╻
;; ┣╸ ┃ ┃┣┳┛┃┃┃┣━┫ ┃ ╺━╸┣━┛┣┳┛┃ ┃┣━┛┣━┫┣╸ ┃  ┗┳┛╺━╸┃  ┣╸ ┃  ┃
;; ╹  ┗━┛╹┗╸╹ ╹╹ ╹ ╹    ╹  ╹┗╸┗━┛╹  ╹ ╹┗━╸┗━╸ ╹    ┗━╸┗━╸┗━╸┗━╸
;; ---------------------------------------------------------------------------
(let ((prev-val     0)
      (up-arrow     "&uarr;")
      (dn-arrow     "&darr;")
      (no-change    "&ndash;"))

  ;; -------------------------------------------------------------------------
  (defun set-prophecy-cell-value (val)
  "
  Sets the value of (what will be) the previous value for use in conjunction
  with later calls to format-prophecy-cell.
  "
    (setq prev-val val))

  ;; -------------------------------------------------------------------------
  (defun get-direction (val)
  "
  Determines whether the passed value rises or falls with respect to prev-val,
  and returns the appropriate arrow symbol.
  "
    (let ((delta (- val prev-val)))
      (cond
        ((> delta 0.01) up-arrow)
        ((< delta 0.01) dn-arrow)
        (t              no-change))))

  ;; -------------------------------------------------------------------------
  (defun format-prophecy-cell (proph-val known-val)
  "
  Provides the formatting for a prophecy value in a table cell, where the actual
  value may or may not be known.
  "
    (if (numberp known-val)
        (let* ((err-amt     (abs (- proph-val known-val)))
               (err-pct     (* (/ err-amt known-val) 100))
               (proph-dir   (get-direction proph-val))
               (known-dir   (get-direction known-val))
               (dir-colour  (if (string= proph-dir known-dir) "#00FF00" "#FF0000")))
          ;(log-debug "Prophecy: prev[~a] sibyl[~a:~a] real[~a:~a]"
          ;           prev-val
          ;           proph-val proph-dir
          ;           known-val known-dir)
          (setq prev-val known-val)
          (format nil "[~1$%] ~$ <div style='color:~a; display:inline'><b>~a~a</b></div> ~$"
                      err-pct
                      proph-val
                      dir-colour
                      proph-dir
                      known-dir
                      known-val))
        (progn
          ;(log-debug "PROPH: ~a" known-val)
          (setq prev-val proph-val)
          (format nil "~$" proph-val)))))


;; ---------------------------------------------------------------------------
;; ┏━╸┏━┓┏━╸┏━┓╺┳╸┏━╸   ┏━┓┏━┓┏━┓┏━┓╻ ╻┏━╸┏━╸╻ ╻   ╻ ╻╺┳╸┏┳┓╻
;; ┃  ┣┳┛┣╸ ┣━┫ ┃ ┣╸ ╺━╸┣━┛┣┳┛┃ ┃┣━┛┣━┫┣╸ ┃  ┗┳┛╺━╸┣━┫ ┃ ┃┃┃┃
;; ┗━╸╹┗╸┗━╸╹ ╹ ╹ ┗━╸   ╹  ╹┗╸┗━┛╹  ╹ ╹┗━╸┗━╸ ╹    ╹ ╹ ╹ ╹ ╹┗━╸
;; ---------------------------------------------------------------------------
(defun create-prophecy-html (prophs proph-days web-path &optional (fname "index"))
"
Creates an HTML file showing the prophecy graph represented by an array of prophecy run files
"
  (declare (type (simple-array prophecy (*)) prophs))

  (log-info "Working in ~a" web-path)

  ; The symbol and the trade-date will be the same in all prophecies in the array
  (let* ((local-work      (equal web-path *default-pathname-defaults*))
         (symbol          (prophecy-symbol (aref prophs 0)))
         (trade-yyyymmdd  (prophecy-trade-date (aref prophs 0)))
         (trade-date      (yyyymmdd-to-date trade-yyyymmdd))
         (known-data      (sql-known-prices  symbol
                                            (dts:time-subtract trade-date *KNOWN-DATA-INTERVAL*)
                                            trade-date))
         (now-known-data  (sql-now-known-prices symbol trade-date))
         (rev-proph-data  '())
         (target-web-dirl (if local-work (pathname-directory (fad:pathname-as-directory web-path))
                                         (append (pathname-directory web-path)
                                                 (list symbol (format nil "~a+~d/"
                                                                          trade-yyyymmdd
                                                                          proph-days)))))
         (origin-web-dirl (if local-work target-web-dirl (pathname-directory web-path)))
         (paean-filepath  (when (and local-work
                                     (>= (length now-known-data) proph-days))
                            (make-pathname :directory target-web-dirl :name "paean" :type "txt")))
         (html-filepath   (make-pathname :directory target-web-dirl :name fname :type "html"))
         (title           (format nil "Prophecy: ~a ~a +~d"
                                      symbol
                                      (yyyymmdd-to-date-text trade-yyyymmdd)
                                      proph-days))
         (first-time?     t))

   ;(log-debug "Origin web: ~a" origin-web-dirl)
   ;(log-debug "Target web: ~a" target-web-dirl)
   ;(log-debug "Local work: ~a" local-work)
   ;(log-debug "Now known data: ~a" now-known-data)

    ; If we're resinging locally, make sure we keep an original copy of sibyl's index
    (when (and local-work
               (string= fname "index"))
      (let ((backup (make-pathname :directory target-web-dirl :name "index.orig" :type "html")))
        (unless (fad:file-exists-p backup)
          (log-info "Backing up to ~a" backup)
          (fad:copy-file html-filepath backup))))

    ; Got what we need to build the HTML, so do it!
    (with-open-file (html-out (ensure-directories-exist html-filepath) :direction :output
                                                                       :if-exists :supersede)
      (with-html-output (html-out nil :prologue t :indent t)
        (:html :lang "en"
          (:head
            (:title (str title))
            (:link :rel "stylesheet" :href "../../sibyl.css"       :type "text/css" :media "screen")
            (:link :rel "stylesheet" :href "../../sibyl-print.css" :type "text/css" :media "print")
            (:script :src "../../scripts/raphael.js")
            (:script :src "../../scripts/popup.js")
            (:script :src "../../scripts/jquery.js")
            (:script :src "../../scripts/graph-model.js"))
          (:body
            (:table :id "data"

              ; <tfoot> has the dates
              (:tfoot
                (:tr

                  ; Date codes for the known values
                  (loop for (date nil) in known-data do
                   (set-last-trade-yyyymmdd date)
                    (htm
                      (:td (fmt "~a" (if first-time?
                                         (progn (setq first-time? nil) (yyyymmdd-to-mmm date))
                                         (yyyymmdd-to-graph-code date))))))

                  ; If the model is older, we may now have dates for the prophesized values
                  (loop for (date nil) in now-known-data do
                    (htm
                      (:td (fmt "~a" (yyyymmdd-to-graph-code date)))))

                  ; If it's still in the future, just indicate the number of days out
                  (loop for day from (1+ (length now-known-data)) upto proph-days do
                    (htm
                      (:td (fmt "+~a" day))))))

              ; <tbody> has the closing prices: known AND prophesized
              (:tbody
                (:tr

                 ; Known values from the DB              
                 (loop for (nil price) in known-data do
                   (set-prophecy-cell-value price)
                   (htm
                     (:td (fmt "~$" price))))

                 ; Prophesized values from the *.p files
                 (loop for proph being the elements of prophs do
                   (let* ((proph-filepath   (prophecy-path proph))
                          (days             (prophecy-days proph))
                          (value            -999)
                          (now-known-pair   (when (>= days 1) (nth (1- days) now-known-data)))
                          (now-known-date   (when now-known-pair (car  now-known-pair)))
                          (now-known-value  (when now-known-pair (cadr now-known-pair)))
                          (run-filepath     (make-pathname
                                                :directory origin-web-dirl
                                                :type      "html"
                                                :name      (format nil "run.~a.~a.~d.~a"
                                                                       symbol
                                                                       *SIBYL-TARGET*
                                                                       days
                                                                       trade-yyyymmdd))))
                     ; Output the HTML
                     (with-open-file (value-in proph-filepath)
                       (setq value (read value-in))
                       (htm (:td (fmt "~$" value)))                         ; write data table for graph
                       (setq rev-proph-data                                 ; Save for price table below
                             (cons (list (format-prophecy-date days  now-known-date)
                                         (format-prophecy-cell value now-known-value))
                                   rev-proph-data)))

                      ; On the last run, we output a paean.txt to show we're done
                      (when paean-filepath
                        (with-open-file (results paean-filepath :direction         :output
                                                                :if-does-not-exist :create
                                                                :if-exists         :append)
                          (format results "~a,~a,~$,~$~%" days now-known-date value now-known-value)))
                      
                      ; The .p file and run files get relocated to the HTML directory
                      ; It may be a bit messy to do this in the middle of building the HTML file,
                      ; but we've got all the pieces handy
                      (unless local-work
                        (progn
                          (rename-file proph-filepath (make-pathname :directory target-web-dirl
                                                                     :name      (pathname-name proph-filepath)
                                                                     :type      "p"))
                          (rename-file run-filepath   (make-pathname :directory target-web-dirl
                                                                     :name      (pathname-name run-filepath)
                                                                     :type      "html")))))))))

            (:h2 (str title))
            (str (create-date-price-table symbol trade-yyyymmdd known-data (reverse rev-proph-data)))
            (:div :id "holder")))))

    (log-info "Created ~a" (namestring html-filepath))

    ; Add softlinks to common web resources
    (unless local-work
      (let ((html-dir (directory-namestring html-filepath)))
        (sb-ext:run-program "/bin/ln" (list "-s" "../../scripts"         html-dir))
        (sb-ext:run-program "/bin/ln" (list "-s" "../../images"          html-dir))
        (sb-ext:run-program "/bin/ln" (list "-s" "../../sibyl.css"       html-dir))
        (sb-ext:run-program "/bin/ln" (list "-s" "../../sibyl-print.css" html-dir))))

    html-filepath))


;; ---------------------------------------------------------------------------
;; ┏━┓┏━┓┏━┓┏━╸┏━╸┏━┓┏━┓   ┏━┓┏━┓┏━┓┏━┓╻ ╻┏━╸┏━╸╻┏━╸┏━┓
;; ┣━┛┣┳┛┃ ┃┃  ┣╸ ┗━┓┗━┓╺━╸┣━┛┣┳┛┃ ┃┣━┛┣━┫┣╸ ┃  ┃┣╸ ┗━┓
;; ╹  ╹┗╸┗━┛┗━╸┗━╸┗━┛┗━┛   ╹  ╹┗╸┗━┛╹  ╹ ╹┗━╸┗━╸╹┗━╸┗━┛
;; ---------------------------------------------------------------------------
(defun process-prophecies (proph-path web-path &optional (html-name "index"))
"
Turns all of the prophecy files in *PROPHECY-PATH* into HTML files off of *WEB-PATH*
"
  (ssql:with-connection (list "delphi" "dbuser" "dbpass" "dbhost")
    (do* ((parsed-prophs (extract-prophecies (mapcar #'path-to-prophecy (directory proph-path)))
                         (extract-prophecies remaining))
          (proph-arr     (car   parsed-prophs) (car   parsed-prophs))
          (proph-cnt     (cadr  parsed-prophs) (cadr  parsed-prophs))
          (remaining     (caddr parsed-prophs) (caddr parsed-prophs))
          (cnt           0                     (1+ cnt)))
       
         ; Go until we run out of prohecy files
         ((and (eq '!INCOMPLETE! proph-arr)
               (<  (length remaining) +MIN-PROPHECY-DAYS+))
          cnt)

      ; Body: turn the .p files into a .html file
      (unless (eq '!INCOMPLETE! proph-arr)
        (create-prophecy-html proph-arr proph-cnt web-path html-name)))))


;; ---------------------------------------------------------------------------
;; ╺┳╸╻┏━╸╻┏ ┏━╸┏━┓┏━┓
;;  ┃ ┃┃  ┣┻┓┣╸ ┣┳┛ ╺┛
;;  ╹ ╹┗━╸╹ ╹┗━╸╹┗╸ ╹
;; ---------------------------------------------------------------------------
(defun ticker? (symb)
 "
 Returns T if SYMB is a valid ticker (stock) symbol, nil otherwise.
 "
  (declare (type string symb))

  ; Tickers are 1-4 characters, all uppercase
  (let ((len (length symb)))
    (and (>= len 1)
         (<= len 4)
         (every (lambda (ch) (and (char>= ch #\A)
                                  (char<= ch #\Z)))
                 symb))))


;; ---------------------------------------------------------------------------
;; ┏━┓ ╻ ┏┓╻ ┏━╸
;; ┗━┓ ┃ ┃┗┫ ┃╺┓
;; ┗━┛ ╹ ╹ ╹ ┗━┛
;; ---------------------------------------------------------------------------
(defun sing ()
"
Turns all of the prophecy files in *PROPHECY-PATH* into HTML files off of *WEB-PATH*
"
  (log-notice "Welcome! Sing with us the paean...")
  (process-prophecies *PROPHECY-PATH* *WEB-PATH*))


;; ---------------------------------------------------------------------------
;; ┏━┓ ┏━╸ ┏━┓ ╻ ┏┓╻ ┏━╸
;; ┣┳┛ ┣╸  ┗━┓ ┃ ┃┗┫ ┃╺┓
;; ╹┗╸ ┗━╸ ┗━┛ ╹ ╹ ╹ ┗━┛
;; ---------------------------------------------------------------------------
(defun resing ()
 "
Recreates prophecy HTML with up-to-date information from Delphi.

FIXME: for the moment resing only runs in the current directory,
       we need a plan.
"
  (log-notice "Welcome! Sing with us again the paean...")
  (process-prophecies *PROPHECY-CWD* *default-pathname-defaults*))



;; ---------------------------------------------------------------------------
;; ┏━╸┏━╸╺┳╸   ┏━┓╺┳╸┏━┓┏━╸╻┏    ╺┳┓╻┏━┓┏━┓
;; ┃╺┓┣╸  ┃ ╺━╸┗━┓ ┃ ┃ ┃┃  ┣┻┓╺━╸ ┃┃┃┣┳┛┗━┓
;; ┗━┛┗━╸ ╹    ┗━┛ ╹ ┗━┛┗━╸╹ ╹   ╺┻┛╹╹┗╸┗━┛
;; ---------------------------------------------------------------------------
(defun get-stock-dirs (fpaths)
 "
 Returns a list of pathnames to all the directories directly under FPATH, which
 are named as stock ticker symbols (as best we can tell).
 "
  (declare (type list fpaths))

  (remove-if-not (lambda (pn)
                   (ticker? (car (last (pathname-directory pn)))))
                 fpaths))


;; ---------------------------------------------------------------------------
;; ╻ ╻┏━┓╺┳┓┏━┓╺┳╸┏━╸   ┏┳┓┏━┓╺┳┓┏━╸╻  ┏━┓
;; ┃ ┃┣━┛ ┃┃┣━┫ ┃ ┣╸ ╺━╸┃┃┃┃ ┃ ┃┃┣╸ ┃  ┗━┓
;; ┗━┛╹  ╺┻┛╹ ╹ ╹ ┗━╸   ╹ ╹┗━┛╺┻┛┗━╸┗━╸┗━┛
;; ---------------------------------------------------------------------------
(defun update-models (dir)
"
Handles updates for a series of models for one stock in a directory such as:

    #p«/path/to/www/AAPL/»
"
  (log-debug "Updating models in ~a" dir)
  (let ((models (remove-if-not
                  (lambda (subdir)
                    (when (fad:directory-pathname-p subdir)
                      (let* ((name (car (last (pathname-directory  subdir))))
                             (len-1 (1- (length name))))
                        (and (>= len-1 10)
                             (eq #\2 (char name 0))        ; 20YYMMDD+NN
                             (eq #\0 (char name 1))
                             (eq #\+ (char name 8))
                             (loop for ndx from 2 upto 7     always (digit-char-p (char name ndx)))
                             (loop for ndx from 9 upto len-1 always (digit-char-p (char name ndx)))))))
                  (directory (merge-pathnames #p"*" dir)))))

    ;(log-debug "MODELS: ~a" models)
    (loop for model in models summing

      (if (fad:file-exists-p (merge-pathnames #p"paean.txt" model))

        ; Indicator file means there's no work left to do in this one
        (prog1 0 (log-info "Already sang in ~a" model))

        ; Direcory in need of processing
        ;
        ; FIXME: The CWD built-in is not scoping dynamically
        (let ((*default-pathname-defaults* model))
          (resing)))
      into cnt
      finally (return cnt))))


;; ---------------------------------------------------------------------------
;; ┏━╸╻ ╻┏━┓┏━┓╻ ╻┏━┓
;; ┃  ┣━┫┃ ┃┣┳┛┃ ┃┗━┓
;; ┗━╸╹ ╹┗━┛╹┗╸┗━┛┗━┛
;; ---------------------------------------------------------------------------
(defun chorus ()
"
Runs the workspace of Sibyl, singing and resinging the results into glorious
and (hopefully) useful HTML drill-down charts.
"
  (log-notice "Starting to chorus for Sibyl")

    ; Put together new models
    (sing)
    
    ; Handle models already built
    (loop for dir in (get-stock-dirs (directory *WEB-PATH*)) summing
      (update-models dir)
      into cnt
      finally (return cnt)))
