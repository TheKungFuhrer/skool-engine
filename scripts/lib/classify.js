/**
 * Classification logic based ONLY on a lead's own inbound messages.
 * Tags, custom fields, and pipeline stages are COMPLETELY IGNORED.
 */

function classifyFromInboundMessages(inboundTexts, currentCategory) {
  if (!inboundTexts || inboundTexts.length === 0) return { category: null, reasoning: '' };

  // Skip classification-irrelevant messages
  const skipPatterns = ['stop', 'unsubscribe', 'remove', 'opt out', 'please remove'];
  const meaningful = inboundTexts.filter(t => {
    const lower = t.toLowerCase().trim();
    return lower.length > 2 && !skipPatterns.some(p => lower === p);
  });

  if (meaningful.length === 0) return { category: null, reasoning: '' };

  const meaningfulCombined = meaningful.join(' ').toLowerCase();

  // --- Active venue owner signals ---
  const venueOwnerPatterns = [
    /i (?:own|have|run|manage|operate) (?:a |an |my |the |our )?(?:venue|event (?:space|center|hall)|banquet|ballroom|reception hall|wedding venue|barn|estate|manor)/i,
    /my (?:venue|event (?:space|center|hall)|banquet|ballroom|property)/i,
    /our (?:venue|event (?:space|center|hall)|banquet|ballroom)/i,
    /(?:i'?m |i am )(?:a )?venue owner/i,
    /already renting/i,
    /(?:i |we )(?:already |currently )?(?:host|book|rent out) (?:events|weddings|parties)/i,
    /(?:had|have) (?:a |an |my |our )?(?:event )?(?:hall|venue|space) for \d+ ?y/i,
    /(?:we|i) (?:do|get|have) (?:about |around )?\d+ (?:events?|bookings?|weddings?) (?:a |per )/i,
    /(?:i |we )(?:just )?opened (?:a |my |our )?(?:venue|space|hall)/i,
    // Additional patterns to catch missed cases
    /(?:i'?m |i am |yes[, ]*(?:i'?m |i am )?)?(?:already )?renting/i,
    /(?:been|currently) renting/i,
    /(?:i |we )(?:have|got) (?:a |an |my |our |the )?property/i,
    /(?:currently |already )?in business/i,
    /renting (?:out )?(?:my |our )(?:space|property|venue|backyard|building|hall)/i,
    /(?:i |we )(?:just )?opened (?:our |my )?doors/i,
  ];

  for (const pattern of venueOwnerPatterns) {
    if (pattern.test(meaningfulCombined)) {
      return {
        category: 'active_venue_owner',
        reasoning: `Lead's own words indicate active venue ownership: "${meaningful.find(t => pattern.test(t.toLowerCase())) || meaningful[0]}"`,
      };
    }
  }

  // --- Aspiring venue owner signals ---
  const aspiringPatterns = [
    /(?:looking|want|trying|planning|hoping|ready) to (?:get started|start|open|launch|build|create|find)/i,
    // Context-aware "get started" — only match when venue/property context exists
    /get(?:ting)? started.{0,80}(?:venue|space|property|location|building|rent|commercial|residential)/i,
    /(?:venue|space|property|location|building|rent|commercial|residential).{0,80}get(?:ting)? started/i,
    /(?:don'?t|do not) have (?:a )?(?:venue|space|property) yet/i,
    /looking for (?:a )?(?:property|venue|space|location|building)/i,
    /(?:want|hope|plan) to (?:have|open|start) (?:a |my |our )?(?:venue|event|space)/i,
    /aspiring/i,
    /(?:i'?m |i am )(?:just )?getting started/i,
    /not (?:yet|currently) (?:renting|operating|open)/i,
    /in the process of/i,
    /(?:looking|searching) for (?:a )?(?:commercial|residential) (?:property|space|building)/i,
  ];

  for (const pattern of aspiringPatterns) {
    if (pattern.test(meaningfulCombined)) {
      // Don't reclassify someone who's already active_venue_owner to aspiring
      if (currentCategory === 'active_venue_owner') {
        return { category: null, reasoning: '' };
      }
      return {
        category: 'aspiring_venue_owner',
        reasoning: `Lead's own words indicate aspiring venue ownership: "${meaningful.find(t => pattern.test(t.toLowerCase())) || meaningful[0]}"`,
      };
    }
  }

  // --- Service provider signals ---
  const serviceProviderPatterns = [
    /(?:i |we )(?:am |are )?(?:a )?(?:photographer|dj|caterer|florist|planner|decorator|videographer|officiant|bartender|stylist)/i,
    /(?:i |we )(?:do |provide |offer )(?:photography|catering|floral|flowers|decorat|planning|video|bartending|hair|makeup|dj)/i,
    /(?:my |our )(?:photography|catering|floral|decor|planning|dj|video|photo ?booth|bakery|cake) (?:business|company|service)/i,
    /(?:i |we )(?:have|own|run|operate) (?:a )?(?:photo ?booth|bakery|cake|catering|floral|decor|planning|dj|rental) (?:business|company|service)?/i,
    /rent(?:ing)? out (?:my |our )?(?:equipment|tables|chairs|linens|photo ?booth|bounce|inflatab)/i,
    /(?:i |we )(?:sell|make|bake|create) (?:cakes?|desserts?|flowers?|arrangements?)/i,
    // Additional patterns
    /(?:i |we )?(?:am |are )?(?:an )?event (?:planner|coordinator|designer)/i,
    /(?:i |we )?(?:have |)?(?:purchased|bought) (?:a )?(?:photo ?booth)/i,
    /(?:looking to |want to )?rent(?:ing)? out (?:my |our )?equipment/i,
  ];

  for (const pattern of serviceProviderPatterns) {
    if (pattern.test(meaningfulCombined)) {
      return {
        category: 'service_provider',
        reasoning: `Lead's own words indicate service provider: "${meaningful.find(t => pattern.test(t.toLowerCase())) || meaningful[0]}"`,
      };
    }
  }

  // --- "Other" signals (clearly not in the industry) ---
  const otherPatterns = [
    /(?:class|education|learn|course|school|training)/i,
    /(?:not interested|wrong number|who is this|don'?t know)/i,
    /(?:air ?b.?n.?b|airbnb|short.term rental)/i,
  ];

  if (currentCategory !== 'other') {
    for (const pattern of otherPatterns) {
      if (pattern.test(meaningfulCombined)) {
        if (meaningfulCombined.includes('class') && meaningfulCombined.includes('education') && currentCategory !== 'active_venue_owner') {
          return {
            category: 'other',
            reasoning: `Lead's own words indicate they're interested in classes/education, not venue ownership: "${meaningful[0]}"`,
          };
        }
      }
    }
  }

  return { category: null, reasoning: '' };
}

module.exports = { classifyFromInboundMessages };
