"""Gated DICOM Basic Grayscale Print SCU for the DRYPIX profile.

This module is deliberately opt-in: ``allow_print`` and ``enabled`` must both
be true. It sends one composed raster page into one Basic Film Box, keeping
selection/layout identical to the PDF composer without attempting any printer
connection during normal preview operation.
"""
from __future__ import annotations

import io
from pathlib import Path

from PIL import Image
from pydicom import dcmread
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, SecondaryCaptureImageStorage, generate_uid
from pynetdicom import AE
from pynetdicom.sop_class import (
    BasicGrayscaleImageBoxSOPClass, BasicGrayscalePrintManagementMetaSOPClass,
)


def _image_dataset(path: str) -> Dataset:
    image = Image.open(path).convert("L")
    ds = Dataset()
    ds.SOPClassUID = SecondaryCaptureImageStorage
    ds.SOPInstanceUID = generate_uid()
    ds.PatientName = "FILM"
    ds.PatientID = "FILM"
    ds.StudyInstanceUID = generate_uid()
    ds.SeriesInstanceUID = generate_uid()
    ds.StudyDate = ""
    ds.Modality = "OT"
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.Rows, ds.Columns = image.height, image.width
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.PixelData = image.tobytes()
    return ds


def print_composed_page(image_path: str, profile: dict) -> dict:
    if not profile.get("enabled") or not profile.get("allow_print"):
        raise RuntimeError("DRYPIX printing is disabled")
    ae = AE(ae_title=str(profile.get("calling_ae", "ORTHANC")))
    ae.add_requested_context(BasicGrayscalePrintManagementMetaSOPClass)
    association = ae.associate(profile["host"], int(profile.get("port", 104)),
                               ae_title=str(profile.get("called_ae", "DRYPIX")))
    if not association.is_established:
        raise RuntimeError("DRYPIX association failed")
    try:
        session = Dataset()
        session.NumberOfCopies = "1"
        session.PrintPriority = "MED"
        session.MediumType = "BLUE FILM"
        session.FilmDestination = "PROCESSOR"
        status, result = association.send_n_create(
            BasicGrayscalePrintManagementMetaSOPClass, session, meta_uid=generate_uid())
        if status is None or status.Status not in (0x0000,):
            raise RuntimeError(f"Film session N-CREATE failed: {getattr(status, 'Status', None)}")
        film_box_uid = getattr(result, "ReferencedFilmBoxSequence")[0].ReferencedSOPInstanceUID
        box = Dataset()
        box.ImageDisplayFormat = "STANDARD\\1,1"
        box.FilmOrientation = profile.get("film_orientation", "PORTRAIT")
        box.FilmSizeID = profile.get("film_size", "14INX17IN")
        box.MagnificationType = "NONE"
        box.BorderDensity = "BLACK"
        box.EmptyImageDensity = "BLACK"
        status, result = association.send_n_create(
            BasicGrayscalePrintManagementMetaSOPClass, box, meta_uid=film_box_uid)
        if status is None or status.Status not in (0x0000,):
            raise RuntimeError(f"Film box N-CREATE failed: {getattr(status, 'Status', None)}")
        image_uid = result.ReferencedImageBoxSequence[0].ReferencedSOPInstanceUID
        image_ds = _image_dataset(image_path)
        status = association.send_n_set(BasicGrayscaleImageBoxSOPClass, image_ds, meta_uid=image_uid)[0]
        if status is None or status.Status not in (0x0000,):
            raise RuntimeError(f"Image box N-SET failed: {getattr(status, 'Status', None)}")
        status = association.send_n_action(BasicGrayscalePrintManagementMetaSOPClass, Dataset(), 1, meta_uid=film_box_uid)[0]
        if status is None or status.Status not in (0x0000,):
            raise RuntimeError(f"Film print N-ACTION failed: {getattr(status, 'Status', None)}")
        return {"ok": True, "film_size": profile.get("film_size"), "layout": profile.get("film_layout", "6x4")}
    finally:
        association.release()
