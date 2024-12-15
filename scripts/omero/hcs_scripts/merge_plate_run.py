#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 merge_plate_run.py"

 Merge the plate acquisitions of different plates.

-----------------------------------------------------------------------------
  Copyright (C) 2018 - 2024
  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
------------------------------------------------------------------------------
Created by Tom Boissonnet

"""

from omero.gateway import BlitzGateway
import omero
from omero.model import WellI
from omero.rtypes import rlong, rstring, robject
import omero.scripts as scripts


P_DTYPE = "Data_Type"  # Do not change
P_IDS = "IDs"  # Do not change
P_TARGET_PLATE_ID = "Target Plate ID"
P_SORTING = "Order runs by"


def combine_plates(conn, target_plate_id, source_ids, source_type,
                   sort_way, same_screen=True):
    """
    Move the plate acquisitions of the source plate to the target
    plate. When wells are missing from the target plate, new wells
    are created in the target plate. The order in which the plates are merged matters,
    and is affected by the sort_run_names parameter.
    parameters:
      - conn: connection object to OMERO
      - target_plate: the ID of the plate into which the source_plates are merged
      - source_ids. iterable of plate IDs to merge into the target plate
      - sort_run_names: process the runs of the given plates in alphabetical order
      - safety_screen: Safety to ensure that the plates processed are all linked to the same screen
    """
    update_service = conn.getUpdateService()

    if type(source_ids) is int:
        source_ids = [source_ids]

    target_plate = conn.getObject("Plate", target_plate_id)
    assert target_plate is not None, f"Target Plate:{target_plate} not found."

    plate_run_l = []
    if source_type == "Plate":
        # Make sure target is not in source_ids
        source_ids = set(source_ids).difference({target_plate_id})

        for plate_id in source_ids:
            # Verification that all plates have runs; if not create one.
            plate_o = conn.getObject("Plate", plate_id)
            if len(list(plate_o.listPlateAcquisitions())) == 0:
                # No Run on this Plate
                plate_acq_o = omero.model.PlateAcquisitionI()
                plate_acq_o.name = omero.rtypes.RStringI(plate_o.getName())
                plate_acq_o.plate = omero.model.PlateI(plate_o.getId(), False)

                all_ws = []
                for well in plate_o.listChildren():
                    for ws in well.listChildren():
                        all_ws.append(ws._obj)
                plate_acq_o.addAllWellSampleSet(all_ws)
                update_service.saveObject(plate_acq_o)

        # Reload all objects and create (plate_obj, run_obj) tuples
        for plate_o in conn.getObjects("Plate", source_ids):
            plate_run_l.extend([(plate_o, run_o) for run_o in plate_o.listPlateAcquisitions()])
    else:
        plate_run_l = [(run_o.getParent(), run_o) for run_o in conn.getObjects("PlateAcquisition", source_ids)]

    if sort_way == "Plate & run name":
        plate_run_l = sorted(plate_run_l, key=lambda x: (x[0].getName(), x[1].getName()))
    elif sort_way == "Acquisition name":
        plate_run_l = sorted(plate_run_l, key=lambda x: x[1].getName())
    elif sort_way == "Acquisition start time":
        all_time = [r.getStartTime() for p, r in plate_run_l]
        assert None not in all_time, "Some runs don't have a start acquisition time."
        plate_run_l = sorted(plate_run_l, key=lambda x: x.getStartTime())

    print("\n".join([pr[1].getName() for pr in plate_run_l]))

    if same_screen:
        screen_l = []
        plate_l = []
        for plate, _ in plate_run_l + [(target_plate, None)]:
            screen = plate.getParent()
            if screen is None:
                plate_l.append(plate.getId())
                continue
            screen_l.append(screen.getId())
        screen_l = list(set(screen_l))

        ids = ', '.join(map(str, plate_l))
        assert len(plate_l) == 0, f"Screen safety error: plate {ids} not part of a screen"
        assert len(screen_l) == 1, f"Screen safety error: Plates belong to different Screens, {screen_l}"

    well_d = {}
    for well in target_plate.listChildren():
        well_d[well.getWellPos()] = well._obj

    # Populating target with missing wells
    new_well_count = 0
    for plate, _ in plate_run_l:
        for well in plate.listChildren():
            if well.getWellPos() not in well_d.keys():
                new_well = WellI()
                new_well.setColumn(well._obj.getColumn())
                new_well.setRow(well._obj.getRow())
                new_well.setPlate(target_plate._obj)
                well_d[well.getWellPos()] = update_service.saveAndReturnObject(new_well)
                print(f"Create {well.getWellPos()}")
                new_well_count += 1

    count_well_sample = 0
    for plate, run in plate_run_l:
        for well in plate.listChildren():
            well_oi = well_d[well.getWellPos()]

            for ws in filter(lambda x: x._obj.plateAcquisition._id._val == run.getId(),
                             well.listChildren()):
                ws._obj.setWell(well_oi)
                well_oi.addWellSample(ws._obj)
                count_well_sample += 1

        for _, well_oi in well_d.items():
            _ = update_service.saveAndReturnObject(well_oi)

        run._obj.setPlate(target_plate._obj)
        _ = update_service.saveAndReturnObject(run._obj)

        # Need to reload all target objects here
        target_plate = conn.getObject("Plate", target_plate.getId())
        well_d = {}
        for well in target_plate.listChildren():
            well_d[well.getWellPos()] = well._obj

    print("\n------------------------------------\n")
    message = (f"{count_well_sample} Images from {len(plate_run_l)} " +
               f"Runs merged into Plate:{target_plate.getId()}.")
    if new_well_count > 0:
        message += f" {new_well_count} Wells created."

    return message, target_plate


def run_script():
    """
    Main entry point, called by the client to initiate the script, collect
    parameters, and execute annotation deletion based on user input.

    :return: Sets output messages and result objects for OMERO client session.
    :rtype: None
    """
    source_types = [
        rstring("Plate"),
        rstring("Acquisition")
    ]

    sort_ways = [
        rstring("Plate name"),
        rstring("Acquisition name"),
        rstring("Acquisition start time")
    ]

    # Here we define the script name and description.
    # Good practice to put url here to give users more guidance on how to run
    # your script.
    client = scripts.client(
        'Merge plate & run',
        """
    Merge several plates into the first (sorted alphabetically).
    \t
        """,  # Tabs are needed to add line breaks in the HTML

        scripts.Int(
            P_TARGET_PLATE_ID, optional=False, grouping="1",
            description="The plate/runs bellow are merged to this plate ID."),

        scripts.String(
            P_DTYPE, optional=False, grouping="1.1",
            description="Data type to merge.",
            values=source_types, default="Plate"),

        scripts.List(
            P_IDS, optional=False, grouping="1.2",
            description="IDs of the plates or runs to move " +
                        f"to '{P_TARGET_PLATE_ID}'.").ofType(rlong(0)),

        scripts.String(
            P_SORTING, optional=False, grouping="1.3", values=sort_ways,
            description="IDs of the plates or runs to move " +
                        f"to '{P_TARGET_PLATE_ID}'."),

        authors=["Tom Boissonnet"],
        institutions=["Center for Advanced imaging, HHU"],
        contact="https://forum.image.sc/tag/omero",
        version="1.0.0",
    )

    try:
        params = {}
        for key in client.getInputKeys():
            if client.getInput(key):
                # unwrap rtypes to String, Integer etc
                params[key] = client.getInput(key, unwrap=True)
        print("Input parameters:")
        keys = [P_DTYPE, P_IDS, P_TARGET_PLATE_ID, P_SORTING]
        for k in keys:
            print(f"\t- {k}: {params[k]}")
        print("\n####################################\n")

        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)
        message, robj = combine_plates(conn, params[P_TARGET_PLATE_ID],
                                       params[P_IDS], params[P_DTYPE],
                                       params[P_SORTING])
        client.setOutput("Message", rstring(message))
        if robj is not None:
            client.setOutput("Result", robject(robj._obj))
    except AssertionError as err:
        # Display assertion errors in OMERO.web activities
        client.setOutput("ERROR", rstring(err))
        raise AssertionError(str(err))
    finally:
        client.closeSession()


if __name__ == "__main__":
    run_script()