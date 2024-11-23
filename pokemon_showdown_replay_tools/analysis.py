import re
import numpy as np


def parse_replay(replay: str) -> dict:
    """
    Parses a Pokemon Showdown replay log in order to extract information,
    such as which pokemon appeared. Returns parsed data as a dictionary.
    For details about the replay log format, see:
        https://github.com/smogon/pokemon-showdown/blob/master/sim/SIM-PROTOCOL.md
    """
    lines = replay.splitlines()
    pokemon = []
    players = {}
    switch_stmt = re.compile(r'\|(switch|drag)\|(?P<pokemon>[^|]+)\|(?P<details>[^|]+)\|[^|]+')
    replace_stmt = re.compile(r'\|replace\|(?P<pokemon>[^|]+)\|(?P<details>[^|]+)\|[^|]+')
    win_stmt = re.compile(r'\|win\|(?P<user>[^|]+)')
    tie_stmt = re.compile(r'\|tie$')
    player_stmt = re.compile(r'\|player\|p(?P<num>\d)\|(?P<name>[^|]+)\|[^|]+\|[^|]*$')
    pokemon_substmt = re.compile(r'p(\d)(\w): (?P<name>.*)')
    winner = None
    tie = False
    for line in lines:
        mo = switch_stmt.match(line)
        mo = mo or replace_stmt.match(line)
        if mo:
            poke_mo = pokemon_substmt.match(mo.group('pokemon'))
            details = mo.group('details').split(',')
            pokemon.append({
                "player": int(poke_mo.group(1)),
                "position": poke_mo.group(2),
                "name": details[0],
            })
            continue
        
        mo = player_stmt.match(line)
        if mo:
            players.update({int(mo.group('num')): mo.group('name')})
            continue
        
        mo = win_stmt.match(line)
        if mo:
            winner = mo.group('user')
            continue

        mo = tie_stmt.match(line)
        if mo:
            tie = True
            continue
    
    for p in pokemon:
        key = p['player']
        p['player'] = players[key]
    
    return {
        'pokemon': pokemon,
        'winner': winner,
        'tie': tie,
    }