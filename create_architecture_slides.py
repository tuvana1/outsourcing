"""Create a Google Slides deck with outreach automation architecture flowchart."""

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/drive",
]

creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
slides_svc = build("slides", "v1", credentials=creds)
drive_svc = build("drive", "v3", credentials=creds)

# --- helpers ---
EMU = 914400  # 1 inch in EMU

def emu(inches):
    return int(inches * EMU)

def rgb(hex_color):
    h = hex_color.lstrip("#")
    return {
        "red": int(h[0:2], 16) / 255,
        "green": int(h[2:4], 16) / 255,
        "blue": int(h[4:6], 16) / 255,
    }

def make_box(page_id, obj_id, x, y, w, h, text, bg_color, font_size=11, bold=False, font_color="#FFFFFF"):
    """Return requests to create a rounded rectangle with centered text."""
    return [
        {
            "createShape": {
                "objectId": obj_id,
                "shapeType": "ROUND_RECTANGLE",
                "elementProperties": {
                    "pageObjectId": page_id,
                    "size": {"width": {"magnitude": emu(w), "unit": "EMU"},
                             "height": {"magnitude": emu(h), "unit": "EMU"}},
                    "transform": {
                        "scaleX": 1, "scaleY": 1,
                        "translateX": emu(x), "translateY": emu(y),
                        "unit": "EMU",
                    },
                },
            }
        },
        {
            "updateShapeProperties": {
                "objectId": obj_id,
                "fields": "shapeBackgroundFill.solidFill.color",
                "shapeProperties": {
                    "shapeBackgroundFill": {
                        "solidFill": {"color": {"rgbColor": rgb(bg_color)}}
                    }
                },
            }
        },
        {
            "updateShapeProperties": {
                "objectId": obj_id,
                "fields": "outline",
                "shapeProperties": {
                    "outline": {"propertyState": "NOT_RENDERED"}
                },
            }
        },
        {
            "insertText": {
                "objectId": obj_id,
                "text": text,
                "insertionIndex": 0,
            }
        },
        {
            "updateTextStyle": {
                "objectId": obj_id,
                "fields": "foregroundColor,fontSize,bold,fontFamily",
                "style": {
                    "foregroundColor": {"opaqueColor": {"rgbColor": rgb(font_color)}},
                    "fontSize": {"magnitude": font_size, "unit": "PT"},
                    "bold": bold,
                    "fontFamily": "Inter",
                },
                "textRange": {"type": "ALL"},
            }
        },
        {
            "updateParagraphStyle": {
                "objectId": obj_id,
                "fields": "alignment",
                "style": {"alignment": "CENTER"},
                "textRange": {"type": "ALL"},
            }
        },
    ]

def make_arrow(page_id, obj_id, x1, y1, x2, y2, color="#9CA3AF"):
    """Create a line/arrow connector."""
    return [
        {
            "createLine": {
                "objectId": obj_id,
                "lineCategory": "STRAIGHT",
                "elementProperties": {
                    "pageObjectId": page_id,
                    "size": {
                        "width": {"magnitude": abs(emu(x2 - x1)), "unit": "EMU"},
                        "height": {"magnitude": abs(emu(y2 - y1)), "unit": "EMU"},
                    },
                    "transform": {
                        "scaleX": 1 if x2 >= x1 else -1,
                        "scaleY": 1 if y2 >= y1 else -1,
                        "translateX": emu(min(x1, x2)),
                        "translateY": emu(min(y1, y2)),
                        "unit": "EMU",
                    },
                },
            }
        },
        {
            "updateLineProperties": {
                "objectId": obj_id,
                "fields": "lineFill.solidFill.color,endArrow,weight",
                "lineProperties": {
                    "lineFill": {
                        "solidFill": {"color": {"rgbColor": rgb(color)}}
                    },
                    "endArrow": "OPEN_ARROW",
                    "weight": {"magnitude": 2, "unit": "PT"},
                },
            }
        },
    ]

def make_label(page_id, obj_id, x, y, w, h, text, font_size=9, font_color="#6B7280"):
    """Small text label (no background)."""
    return [
        {
            "createShape": {
                "objectId": obj_id,
                "shapeType": "TEXT_BOX",
                "elementProperties": {
                    "pageObjectId": page_id,
                    "size": {"width": {"magnitude": emu(w), "unit": "EMU"},
                             "height": {"magnitude": emu(h), "unit": "EMU"}},
                    "transform": {
                        "scaleX": 1, "scaleY": 1,
                        "translateX": emu(x), "translateY": emu(y),
                        "unit": "EMU",
                    },
                },
            }
        },
        {
            "insertText": {"objectId": obj_id, "text": text, "insertionIndex": 0}
        },
        {
            "updateTextStyle": {
                "objectId": obj_id,
                "fields": "foregroundColor,fontSize,fontFamily,italic",
                "style": {
                    "foregroundColor": {"opaqueColor": {"rgbColor": rgb(font_color)}},
                    "fontSize": {"magnitude": font_size, "unit": "PT"},
                    "fontFamily": "Inter",
                    "italic": True,
                },
                "textRange": {"type": "ALL"},
            }
        },
        {
            "updateParagraphStyle": {
                "objectId": obj_id,
                "fields": "alignment",
                "style": {"alignment": "CENTER"},
                "textRange": {"type": "ALL"},
            }
        },
    ]


# ============================================================
# Create the presentation
# ============================================================

pres = slides_svc.presentations().create(body={
    "title": "Palm Drive Capital — Outreach Automation Architecture"
}).execute()
pres_id = pres["presentationId"]

# Get the default first slide
first_slide_id = pres["slides"][0]["objectId"]

# ============================================================
# SLIDE 1 — Title
# ============================================================

reqs = []

# Set slide background to dark
reqs.append({
    "updatePageProperties": {
        "objectId": first_slide_id,
        "fields": "pageBackgroundFill.solidFill.color",
        "pageProperties": {
            "pageBackgroundFill": {
                "solidFill": {"color": {"rgbColor": rgb("#0F172A")}}
            }
        },
    }
})

# Title text
reqs += make_label(first_slide_id, "title_main", 1, 1.8, 8, 1,
                   "Outreach Automation Architecture", font_size=32, font_color="#F8FAFC")
reqs[len(reqs)-3]["updateTextStyle"]["style"]["italic"] = False
reqs[len(reqs)-3]["updateTextStyle"]["style"]["bold"] = True

reqs += make_label(first_slide_id, "title_sub", 1, 2.8, 8, 0.6,
                   "Harmonic AI  +  Affinity CRM  +  Lemlist", font_size=16, font_color="#94A3B8")

reqs += make_label(first_slide_id, "title_co", 1, 3.6, 8, 0.5,
                   "Palm Drive Capital", font_size=14, font_color="#64748B")

# ============================================================
# SLIDE 2 — Flowchart
# ============================================================

flow_slide_id = "flow_slide"
reqs.append({
    "createSlide": {
        "objectId": flow_slide_id,
        "insertionIndex": 1,
    }
})
reqs.append({
    "updatePageProperties": {
        "objectId": flow_slide_id,
        "fields": "pageBackgroundFill.solidFill.color",
        "pageProperties": {
            "pageBackgroundFill": {
                "solidFill": {"color": {"rgbColor": rgb("#0F172A")}}
            }
        },
    }
})

# Slide title
reqs += make_label(flow_slide_id, "flow_title", 0.5, 0.2, 9, 0.5,
                   "End-to-End Pipeline", font_size=20, font_color="#F8FAFC")
reqs[len(reqs)-3]["updateTextStyle"]["style"]["italic"] = False
reqs[len(reqs)-3]["updateTextStyle"]["style"]["bold"] = True

# Colors
C_BLUE = "#3B82F6"     # automated steps
C_GREEN = "#10B981"    # human-in-the-loop
C_RED = "#EF4444"      # skip/reject
C_PURPLE = "#8B5CF6"   # output/CRM
C_ORANGE = "#F59E0B"   # outreach

# --- Row 1: Main pipeline (left to right) ---
bw, bh = 1.6, 0.65  # box width/height
y1 = 1.1
gap = 0.45

x_harmonic = 0.3
x_filter = x_harmonic + bw + gap
x_dedupe = x_filter + bw + gap
x_sheet = x_dedupe + bw + gap
x_lemlist = x_sheet + bw + gap

reqs += make_box(flow_slide_id, "b_harmonic", x_harmonic, y1, bw, bh,
                 "Harmonic AI\nSearch", C_BLUE, font_size=10, bold=True)

reqs += make_box(flow_slide_id, "b_filter", x_filter, y1, bw, bh,
                 "Filter &\nScore", C_BLUE, font_size=10, bold=True)

reqs += make_box(flow_slide_id, "b_dedupe", x_dedupe, y1, bw, bh,
                 "Affinity\nDedupe", C_PURPLE, font_size=10, bold=True)

reqs += make_box(flow_slide_id, "b_sheet", x_sheet, y1, bw, bh,
                 "Google\nSheet", C_GREEN, font_size=10, bold=True)

reqs += make_box(flow_slide_id, "b_lemlist", x_lemlist, y1, bw, bh,
                 "Lemlist\nOutreach", C_ORANGE, font_size=10, bold=True)

# Arrows between main boxes
arrow_y = y1 + bh / 2
reqs += make_arrow(flow_slide_id, "a1", x_harmonic + bw, arrow_y, x_filter, arrow_y, "#64748B")
reqs += make_arrow(flow_slide_id, "a2", x_filter + bw, arrow_y, x_dedupe, arrow_y, "#64748B")
reqs += make_arrow(flow_slide_id, "a3", x_dedupe + bw, arrow_y, x_sheet, arrow_y, "#64748B")
reqs += make_arrow(flow_slide_id, "a4", x_sheet + bw, arrow_y, x_lemlist, arrow_y, "#64748B")

# --- Labels above arrows ---
reqs += make_label(flow_slide_id, "l1", x_harmonic + bw - 0.1, y1 - 0.3, bw, 0.25,
                   "1000 URNs", font_size=8, font_color="#94A3B8")
reqs += make_label(flow_slide_id, "l2", x_filter + bw - 0.1, y1 - 0.3, bw, 0.25,
                   "top 100", font_size=8, font_color="#94A3B8")
reqs += make_label(flow_slide_id, "l3", x_dedupe + bw - 0.1, y1 - 0.3, bw, 0.25,
                   "net new", font_size=8, font_color="#94A3B8")
reqs += make_label(flow_slide_id, "l4", x_sheet + bw - 0.1, y1 - 0.3, bw, 0.25,
                   "approved", font_size=8, font_color="#94A3B8")

# --- Badges below boxes (Automated / Manual) ---
reqs += make_label(flow_slide_id, "tag_h", x_harmonic, y1 + bh + 0.05, bw, 0.25,
                   "AUTOMATED", font_size=7, font_color="#3B82F6")
reqs += make_label(flow_slide_id, "tag_f", x_filter, y1 + bh + 0.05, bw, 0.25,
                   "AUTOMATED", font_size=7, font_color="#3B82F6")
reqs += make_label(flow_slide_id, "tag_d", x_dedupe, y1 + bh + 0.05, bw, 0.25,
                   "AUTOMATED", font_size=7, font_color="#8B5CF6")
reqs += make_label(flow_slide_id, "tag_s", x_sheet, y1 + bh + 0.05, bw, 0.25,
                   "HUMAN REVIEW", font_size=7, font_color="#10B981")
reqs += make_label(flow_slide_id, "tag_l", x_lemlist, y1 + bh + 0.05, bw, 0.25,
                   "AUTOMATED", font_size=7, font_color="#F59E0B")

# --- Row 2: Detail boxes ---
y2 = 2.4
dh = 1.0

reqs += make_box(flow_slide_id, "d_harmonic", x_harmonic, y2, bw, dh,
                 "Pre-seed / Seed\n5+ highlights\nGrowing headcount",
                 "#1E293B", font_size=8, font_color="#CBD5E1")

reqs += make_box(flow_slide_id, "d_filter", x_filter, y2, bw, dh,
                 "Exclude geo/industry\nRaise score ranking\nCEO + email required",
                 "#1E293B", font_size=8, font_color="#CBD5E1")

reqs += make_box(flow_slide_id, "d_dedupe", x_dedupe, y2, bw, dh,
                 "Search by domain\nSearch by name\nCheck lists & notes",
                 "#1E293B", font_size=8, font_color="#CBD5E1")

reqs += make_box(flow_slide_id, "d_sheet", x_sheet, y2, bw, dh,
                 "Review metrics\nClear unwanted rows\nAdd to Affinity 1a",
                 "#1E293B", font_size=8, font_color="#CBD5E1")

reqs += make_box(flow_slide_id, "d_lemlist", x_lemlist, y2, bw, dh,
                 "Push to campaign\nSequenced emails\nCEO cold outreach",
                 "#1E293B", font_size=8, font_color="#CBD5E1")

# --- Row 3: Skip path from Affinity dedupe ---
y3 = 3.8
reqs += make_box(flow_slide_id, "b_skip", x_dedupe, y3, bw, 0.5,
                 "SKIP\n(already known)", C_RED, font_size=9, bold=True)
reqs += make_arrow(flow_slide_id, "a_skip", x_dedupe + bw/2, y2 + dh, x_dedupe + bw/2, y3, "#EF4444")

# ============================================================
# SLIDE 3 — Known Gap / Fix
# ============================================================

gap_slide_id = "gap_slide"
reqs.append({
    "createSlide": {
        "objectId": gap_slide_id,
        "insertionIndex": 2,
    }
})
reqs.append({
    "updatePageProperties": {
        "objectId": gap_slide_id,
        "fields": "pageBackgroundFill.solidFill.color",
        "pageProperties": {
            "pageBackgroundFill": {
                "solidFill": {"color": {"rgbColor": rgb("#0F172A")}}
            }
        },
    }
})

reqs += make_label(gap_slide_id, "gap_title", 0.5, 0.3, 9, 0.5,
                   "Known Gap & Fix", font_size=20, font_color="#F8FAFC")
reqs[len(reqs)-3]["updateTextStyle"]["style"]["italic"] = False
reqs[len(reqs)-3]["updateTextStyle"]["style"]["bold"] = True

# Problem box
reqs += make_box(gap_slide_id, "gap_problem", 0.5, 1.2, 4, 1.5,
                 "PROBLEM\n\nNo Affinity check before Lemlist push.\nName mismatch in dedupe can let\nportfolio companies slip through\nto cold outreach.",
                 "#7F1D1D", font_size=11, font_color="#FCA5A5")

# Fix box
reqs += make_box(gap_slide_id, "gap_fix", 5.2, 1.2, 4, 1.5,
                 "FIX\n\nAdd second Affinity gate in\nadd_to_lemlist.py — check every\nlead against CRM before pushing.\nImproved name normalization.",
                 "#14532D", font_size=11, font_color="#86EFAC")

# Arrow between
reqs += make_arrow(gap_slide_id, "gap_arrow", 4.6, 1.95, 5.1, 1.95, "#64748B")

# Before/After flow
y_ba = 3.2
reqs += make_label(gap_slide_id, "ba_before_label", 0.5, y_ba, 4, 0.3,
                   "BEFORE", font_size=10, font_color="#EF4444")
reqs[len(reqs)-3]["updateTextStyle"]["style"]["italic"] = False
reqs[len(reqs)-3]["updateTextStyle"]["style"]["bold"] = True

reqs += make_box(gap_slide_id, "ba_sheet1", 0.5, y_ba + 0.4, 1.6, 0.5,
                 "Sheet", "#1E293B", font_size=10, font_color="#CBD5E1")
reqs += make_arrow(gap_slide_id, "ba_a1", 2.15, y_ba + 0.65, 2.6, y_ba + 0.65, "#64748B")
reqs += make_box(gap_slide_id, "ba_lem1", 2.65, y_ba + 0.4, 1.6, 0.5,
                 "Lemlist", C_ORANGE, font_size=10, bold=True)

reqs += make_label(gap_slide_id, "ba_after_label", 5.2, y_ba, 4, 0.3,
                   "AFTER", font_size=10, font_color="#10B981")
reqs[len(reqs)-3]["updateTextStyle"]["style"]["italic"] = False
reqs[len(reqs)-3]["updateTextStyle"]["style"]["bold"] = True

reqs += make_box(gap_slide_id, "ba_sheet2", 5.2, y_ba + 0.4, 1.2, 0.5,
                 "Sheet", "#1E293B", font_size=10, font_color="#CBD5E1")
reqs += make_arrow(gap_slide_id, "ba_a2", 6.45, y_ba + 0.65, 6.85, y_ba + 0.65, "#64748B")
reqs += make_box(gap_slide_id, "ba_aff2", 6.9, y_ba + 0.4, 1.2, 0.5,
                 "Affinity\nCheck", C_PURPLE, font_size=9, bold=True)
reqs += make_arrow(gap_slide_id, "ba_a3", 8.15, y_ba + 0.65, 8.55, y_ba + 0.65, "#64748B")
reqs += make_box(gap_slide_id, "ba_lem2", 8.6, y_ba + 0.4, 1.2, 0.5,
                 "Lemlist", C_ORANGE, font_size=10, bold=True)

# ============================================================
# Execute all requests
# ============================================================

slides_svc.presentations().batchUpdate(
    presentationId=pres_id,
    body={"requests": reqs},
).execute()

# Share with user
drive_svc.permissions().create(
    fileId=pres_id,
    body={"type": "anyone", "role": "writer"},
).execute()

print(f"\nDone! Presentation created:")
print(f"https://docs.google.com/presentation/d/{pres_id}/edit")
