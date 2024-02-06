# === REDACT v2 ===
from itertools import groupby
import re
import datetime
import time
from collections import defaultdict
import copy
from typing import Union

class Redact_Options:
    types_to_redact = []
    def __init__(self, opt):
        if (opt is not None) and 'types_to_redact' in opt:
            self.types_to_redact = opt['types_to_redact']


def redact_transcript_and_nlp(normalised: dict, nlp: dict, options: Union[dict,None] = None):
    opts = Redact_Options(options)

    turns = normalised['turns_array']

    if normalised["metadata"]["media"]["media_type"] == "voice":
        turns_sorted_by_speaker = sorted(turns, key=lambda turn: (turn['source'], turn['start_time'], turn['end_time']))
    elif normalised["metadata"]["media"]["media_type"] == "chat":
        turns_sorted_by_speaker = sorted(turns, key=lambda turn: (turn['source'], time.mktime(datetime.datetime.fromisoformat(turn['timestamp']).timetuple())))
    else:
        raise Exception("redact_transcript: Transcript is not of 'chat' or 'voice' type")
    
    turns_grouped_by_speaker = [[key,list(turn)] for key, turn in groupby(turns_sorted_by_speaker, lambda turn: turn['source'])]

    required_redaction = False
    redact_timings = [] 

    to_process=[] # to_process=[ (turn_index, char_position_start, char_position_end, word, label/ent-tag, NLP_speaker, NLP_turn_index, optional_corrected_text) ]
    
    redact_entities_by_word=defaultdict(list) # {"Word": [{"type":"TYPE", "turn": 1, "start":1, "end":1}, {"type":"TYPE", "turn": 1, "start":1, "end":1}],"Word2": {"type":"TYPE", "turn": 1, "start":1, "end":1}}
    non_redact_entities_by_word=defaultdict(list) # {"Word": [{"type":"TYPE", "turn": 1, "start":1, "end":1}, {"type":"TYPE", "turn": 1, "start":1, "end":1}],"Word2": {"type":"TYPE", "turn": 1, "start":1, "end":1}}
    
    redact_entities_by_turn=defaultdict(list)
    non_redact_entities_by_turn=defaultdict(list)
    
    #Issues: 
    # - Issue:          "Paris (PERSON) from Paris (GPE)"", will become "[PERSON] from [PERSON]"
    #       Solution:     Confirm all replacements against OTHER NON-redact ents first
    # - Issue:          Replacing with labels messes up indexing during processing steps
    #       Solution:     Dont' replace on first pass. record locations, then replace all on second pass
    
    # REDACT EMAILS
    # Remove Emails at some point
    # replace with ***s to not mess up below passes
    # (OR tag with Spacy Like_Email Matcher attribute and redact as usual) <<----
    
    
    ### Pass #1 - FIND ALL NLP-IDENTIFIED ENTITIES AND CATEGORIZE THEM
    ### Loop through NLP Speakers
    for speaker, speaker_turns in nlp.items():
        for group in turns_grouped_by_speaker:
            if group[0] == speaker:
                correlating_speaker_turns = group[1]
        ### Loop through Speaker Turns
        for nlp_turn_index, nlp_turn in enumerate(speaker_turns):
            correlating_turn_index = correlating_speaker_turns[nlp_turn_index]['turn_index']
            ### Loop through Ents
            for ent in nlp_turn["ents"]:
                ### Record redact Ents (Text, TYPE)
                word_info = {"type":ent["label"], "turn_index":correlating_turn_index, "start":ent["start"], "end":ent["end"]}
                if ent["label"].lower() in [x.lower() for x in opts.types_to_redact]:
                    redact_entities_by_word[ent["text"].lower()].append(word_info)
                else:
                    non_redact_entities_by_word[ent["text"].lower()].append(word_info)
    
    if len(redact_entities_by_word)>0:
        required_redaction=True
    # cprint(redact_entities_by_word,'red')
    # cprint(non_redact_entities_by_word,'blue')
    
    ### Pass #1.5 - PROCESS FINDINGS
    ### Generate regex for matching ALL standalone ents (i.e. If "Jerry" is the only identified entity: ["Hi Jerry, how are you?" -> "Jerry"], ["Hi Jerryboy, how are you?" -> N/A no match])
    re_ents = f'(?:^|(?<=[^a-zA-Z]))({"|".join([f"({re.escape(x)})" for x in sorted(redact_entities_by_word.keys(), reverse=True, key=lambda x: len(x))])})(?:$|(?=[^a-zA-Z]))'
    # print(re_ents,'\n')
    
    ### redact_entities_by_turn=[ "1":{"word":"word","type":"type","start":"start","end":"end"},
    #                             "2":{"word":"word","type":"type","start":"start","end":"end"} ]
    for word, infolist in redact_entities_by_word.items():
        for info in infolist:
            turn_info = {"word":word, "type":info["type"], "start":info["start"], "end":info["end"]}
            redact_entities_by_turn[str(info["turn_index"])].append(turn_info)
    for word, infolist in non_redact_entities_by_word.items():
        for info in infolist:
            turn_info = {"word":word, "type":info["type"], "start":info["start"], "end":info["end"]}
            non_redact_entities_by_turn[str(info["turn_index"])].append(turn_info)

    def convert_to_stars(text):
        return ' '.join([len(e)*'*' for e in text.split()]) # "hi there" -> "** *****"
    def convert_to_label(text, label):
        return ' '.join([label for e in text.split()]) # ("hi there", "[PERSON]")-> "[PERSON] [PERSON]"

    # cprint(redact_entities_by_turn, 'magenta')
    # cprint(non_redact_entities_by_turn, 'magenta')

    if redact_entities_by_turn:

        ### Pass #2 - FIND ALL MATCHING TEXT PARTS AND THEIR LOCATIONS
        ### Loop thought NLP Speakers
        for speaker, speaker_turns in nlp.items():
            for group in turns_grouped_by_speaker:
                if group[0] == speaker:
                    correlating_speaker_turns = group[1]
            ### Loop through Speaker Turns (AGENT + CALLER)
            for nlp_turn_index, nlp_turn in enumerate(speaker_turns):
                ### Find correlating Transcript Turn index
                correlating_turn_index = correlating_speaker_turns[nlp_turn_index]['turn_index']
                transcript_turn = normalised["turns_array"][correlating_turn_index - 1] # Ref, not copy
                ### CONSIDERATION: NLP turn "text" === Transcript "turn_text" exactly. (Only need to check 1 of them)
                ### REGEX MATCH all matching ents in the turn
                matches = re.finditer(f'{re_ents}', nlp_turn["text"], flags=re.IGNORECASE)
                
                ### Store span locations for checking against
                turn_has_redactable=False
                for m in matches:
                    text, start, end = m.group(0), m.start(), m.end()
                    # cprint((text, start, end),'green')
                    ### - check against non-redact-ents from this same turn
                    redactable=True
                    for ent in non_redact_entities_by_turn[str(correlating_turn_index)]:
                        if ent['start'] >= start and ent['end'] <= end:
                            redactable = False
                    if redactable:
                        turn_has_redactable=True
                        ### Pick a LABEL type from the list (not 100% accurate, based on highest frequency)
                        word_by_label_frequency = sorted(redact_entities_by_word[text.lower()], key=lambda x: x["type"])
                        # cprint(word_by_label_frequency, 'yellow')
                        replacement_text_stars = convert_to_stars(text)
                        replacement_text_label = '['+word_by_label_frequency[0]["type"]+']'
                        to_process.append([correlating_turn_index, start, end, text, replacement_text_label, speaker, nlp_turn_index, replacement_text_stars, None])
                ### IF turn has misspellings
                if turn_has_redactable and "misspelled_words" in transcript_turn:
                    for word in transcript_turn["misspelled_words"]:
                        ### Add the corrections into the to_process-queue to be re-constructed later
                        already_exists=False
                        for i, proc in enumerate(to_process):
                            if word["start"] == proc[1] and word["end"] == proc[2]: # Word already in to_process, add the corrected word onto the end
                                to_process[i][-1]=word["corr"] if "corr" in word else word["text"]
                                already_exists=True
                            elif proc[1] <= word["start"] < word["end"] <= proc[2]: # Mispelled word is INSIDE a word already in to_process, ignore it as it will get overwritten
                                already_exists=True
                        if not already_exists:
                            to_process.append([correlating_turn_index, word["start"], word["end"], word["text"], None, None, None, None, word["corr"]  if "corr" in word else word["text"] ])


        # pprint(to_process)

        # SORT and GROUP to_process by turn order
        ### to_process = [ "0":{ [ {word data}, {word data} ] },
        #                  "1":{ [ {word data}, {word data} ] } ]
        to_process=sorted(to_process, key=lambda x: (x[0]))
        to_process=[(key, list(group)) for key,group in groupby(to_process, key=lambda x: x[0])]
        

        # Pass #3 - REPLACE ALL FOUND INSTANCES OF A REDACTABLE ENTITY IN ALL AREAS
        #Loop through to_process sorted by turn order
        for turn, words in to_process:
            # print()
            
            transcript_turn=normalised["turns_array"][turn - 1] # Ref, not copy
            # cprint(f'{transcript_turn["turn_text"]}','white')
            
            # SORT words by word position, descending. This makes replacement go from right-to-left, and doesnt break index positioning
            words=sorted(words, reverse=True, key=lambda x: x[1])
            
            # Find the NLP turn based on the first word in the list that has the right fields (always at least 1)
            w_filter=list(filter(lambda x: bool(x[5]), words)) # Filter by word items that have Speaker field
            nlp_turn=nlp[w_filter[0][5]][w_filter[0][6]] # Get NLP Turn by the first field's Speaker and nlp_index
            
            # IF corrections exist, Reset corrected text back to default as it will be rebuilt with redaction included.
            if "corr_text" in transcript_turn:
                transcript_turn["corr_text"]=copy.deepcopy(transcript_turn["turn_text"])
            
            # Loop through to_process (words to correct)
            for correlating_turn_index, start, end, text, transcript_turn_replacer, speaker, nlp_turn_index, nlp_turn_replacer, correction in words:
                # nlp_turn=None
                
                # cprint((correlating_turn_index, start, end, text, transcript_turn_replacer, speaker, nlp_turn_index, nlp_turn_replacer, correction), 'yellow' if speaker is not None else 'cyan')
                
                # - transcript corr_text
                # This happens first to allow for the case when a sentence is being redacted, but also has spelling corrections to apply that will not be redacted.
                if "corr_text" in transcript_turn:
                    transcript_turn["corr_text"] = f'{transcript_turn["corr_text"][:start]}{convert_to_label(text, transcript_turn_replacer) if transcript_turn_replacer else correction}{transcript_turn["corr_text"][end:]}'
                    # cprint(transcript_turn["corr_text"],'blue')
                    
                # ONLY keep going if the current processing entry is for redaction (not just rebuilding corrections)
                if not speaker: # could also check transcript_turn_replacer, nlp_turn_index, nlp_turn_replacer etc. anything to do with redaction
                    continue
                
                # REDACT Turn-level fields:
                # - Transcript turn_text
                turn_text = transcript_turn["turn_text"]
                transcript_turn["turn_text"] = f'{turn_text[:start]}{convert_to_label(text, transcript_turn_replacer)}{turn_text[end:]}'
                
                # - NLP text
                nlp_text=nlp_turn["text"]
                nlp_turn["text"] = f'{nlp_text[:start]}{nlp_turn_replacer}{nlp_text[end:]}'
                # cprint(transcript_turn["turn_text"],'cyan')


                # REDACT lower level fields
                # - NLP / turn / sentence text field
                for sent in nlp_turn['sents']:
                    if sent['start'] <= start < end <= sent['end']: # ent is entirely within sent (or perfect matches)
                        sent['text'] = f"{sent['text'][:start-sent['start']]}{nlp_turn_replacer}{sent['text'][end-sent['start']:]}"
                    elif  start <= sent['start'] < end <= sent['end']: # ent starts before sent and finishes within sent (or perfect matches)
                        sent['text'] = f"{convert_to_stars(sent['text'][sent['start']-end:])}{sent['text'][end-sent['start']:]}"
                    elif  sent['start'] <= start < sent['end'] <= end: # ent starts within sent and finishes after sent (or perfect matches)
                        sent['text'] = f"{sent['text'][:start-sent['start']]}{convert_to_stars(sent['text'][:sent['end']-start])}"
                # - NLP / turn / entity text field
                for ent in nlp_turn['ents']:
                    if ent['start'] <= start < end <= ent['end']: # redact-ent is entirely within ent (or perfect matches)
                        ent['text'] = f"{ent['text'][:start-ent['start']]}{nlp_turn_replacer}{ent['text'][end-ent['start']:]}"
                    elif start <= ent["start"] < ent["end"] <= end: # ent is entirely within redact-ent (or perfect matches)
                        ent['text'] = convert_to_stars(ent['text'])
                # - NLP / turn / token text+lemma field
                for token in nlp_turn['tokens']:
                    if token['start'] <= start < end <= token['end']: # ent is entirely within token (or perfect matches)
                        token['text'] = f"{token['text'][:start-token['start']]}{nlp_turn_replacer}{token['text'][end-token['start']:]}"
                        token['lemma'] = convert_to_stars(token['lemma'])
                    elif start <= token["start"] < token["end"] <= end: # token is entirely within ent (or perfect matches)
                        token['text'] = convert_to_stars(token['text'])
                        token['lemma'] = convert_to_stars(token['lemma'])

                # - transcript / turn / misspelled words fields (remove completely if misspelled word was redacted)
                ### IF CHAT TRANSCRIPT
                if "misspelled_words" in transcript_turn:
                    delete_list=[]
                    for misspelled_word in transcript_turn["misspelled_words"]:
                        if "start" in misspelled_word and (( start >= misspelled_word['start'] and end <= misspelled_word['end']) or (misspelled_word["start"] >= start and misspelled_word["end"] <= end)):
                            delete_list.append(misspelled_word)
                    for thing in delete_list:
                        transcript_turn["misspelled_words"].remove(thing)
                    # IF there are no more misspelled words left, just remove anything to do with correction
                    if len(transcript_turn["misspelled_words"]) == 0:
                        del transcript_turn["misspelled_words"]
                        if "corr_text" in transcript_turn:
                            del transcript_turn["corr_text"]
                        
                ### Construct holistic struct for matching indexes to char positions
                ### IF VOICE TRANSCRIPT
                redact_windows=[]
                if "words_array" in transcript_turn:
                    # print("voice")
                    ordered_words=[word["word_text"] for word in sorted(transcript_turn["words_array"], key=lambda x: x['word_index'])]
                    for i, word_text in enumerate(ordered_words):
                        word_start = i + sum([len(token) for token in ordered_words[:i]])
                        word_end = word_start + len(word_text)
                        add_to_window=False
                        # - transcript / turn / words text filds (partial replacements, accounting for punctuation)
                        if word_start <= start < end <= word_end: # ent is entirely within word (or perfect matches)
                            # cprint(f'{correlating_turn_index} |{text}| is within |{word_text}| {((start,end),(word_start,word_end))}','green')
                            transcript_turn["words_array"][i]["word_text"] = f"{word_text[:start-word_start]}{convert_to_label(word_text, transcript_turn_replacer)}{word_text[end-word_start:]}"
                            # cprint(f'[:{start-word_start}] then label, then [{end-word_start}:]  {transcript_turn["words_array"][i]["word_text"]}','yellow')
                            add_to_window=True
                        elif start <= word_start < word_end <= end: # word is entirely within ent (or perfect matches)
                            # cprint(f'{correlating_turn_index} |{text}| surrounds |{word_text}| {((start,end),(word_start,word_end))}','green')
                            transcript_turn["words_array"][i]["word_text"] = transcript_turn_replacer
                            # cprint(transcript_turn["words_array"][i]["word_text"],'yellow')
                            add_to_window=True
                        elif start <= word_start < end <= word_end: # ent starts before word and finishes within (or perfect matches)
                            # cprint(f'{correlating_turn_index} |{text}| starts before |{word_text}| {((start,end),(word_start,word_end))}','green')
                            transcript_turn["words_array"][i]["word_text"] = f"{convert_to_label(word_text, transcript_turn_replacer)}{word_text[end-word_start:]}"
                            # cprint(transcript_turn["words_array"][i]["word_text"],'yellow')
                            add_to_window=True
                        elif word_start <= start < word_end <= end: # ent starts within word and finished after (or perfect matches)
                            # cprint(f'{correlating_turn_index} |{text}| ends after |{word_text}| {((start,end),(word_start,word_end))}','green')
                            transcript_turn["words_array"][i]["word_text"] = f"{word_text[:start-word_start]}{convert_to_label(word_text, transcript_turn_replacer)}"
                            # cprint(transcript_turn["words_array"][i]["word_text"],'yellow')
                            add_to_window=True
                        redact_windows.append((add_to_window, sorted(transcript_turn["words_array"], key=lambda x: (x['start_time'], x['end_time']))[i]))
                            
                    # pprint(redact_windows)
                    #Timings
                    redact_windows = [ list(group) for key, group in groupby( redact_windows, lambda word: word[0] ) if key ]
                    # pprint(redact_windows)
                    for redact_window in redact_windows:
                        # redact from start_time of first Word, to end_time of last word in redacted period
                        # cprint(redact_window,'green')
                        redact_timings.append({"start":redact_window[0][-1]['start_time'],"end":redact_window[-1][-1]['end_time']})


            # cprint(f'redacted          : {transcript_turn["turn_text"]}','white')
            # cprint(f'redacted nlp      : {nlp_turn["text"]}','white')
            # if "corr_text" in transcript_turn:
            #     cprint(f'redacted corrected: {transcript_turn["corr_text"]}','white')
    
    return (normalised, nlp, redact_timings, required_redaction)


