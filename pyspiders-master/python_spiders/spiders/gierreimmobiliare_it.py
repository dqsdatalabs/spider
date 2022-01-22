import re

import requests
import scrapy
from scrapy import Request

from ..helper import extract_number_only
from ..loaders import ListingLoader


class GierreimmobiliareItSpider(scrapy.Spider):
    name = 'gierreimmobiliare_it'
    allowed_domains = ['widget.getrix.it']
    start_urls = [
        'https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=612F97B4-6831-477E-A5F7-F792327ED8F6&querystring=%3FCategoria%3D1%26Contratto%3DA&clientId=&clientClassName=&numeroColonne=2&lingua=en&numeroRisultati=10&urlPaginaRisultati=%2Fen%2Ffind-properties&urlPaginaAnnuncio=%2Fen%2Fproperty%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fen%2Fconstruction%2F%7Bidcantiere%7D&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=true&mostraGallery=true&carousel=true&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=true&mostraPrivacy=true&urlPrivacy=%2Fen%2Fprivacy&startRisultati=-1&RewriteProvincia=&RewriteComune=&RewriteQuartiere=&RewriteTipologia=&tolerancePrezzo=0&toleranceSuperficie=0&toleranceLocali=false&annunciSalvati=&_=1636897950578']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        html = response.text
        html = html.strip()
        html = html.replace(" ", "")
        html = html.replace("\\", "")
        html = html.replace("\\t", "")
        html = html.replace("\\n", "")
        html = html.replace("\\r", "")
        html = html.replace("&gt;", ">")
        html = html.replace("&lt;", "<")

        Ids = re.findall('href="/en/property/residential/rent/apartment/firenze-fi/([0-9]+)', html)
        Ids = list(dict.fromkeys(Ids))
        Ids = [
            "https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?apikey=612F97B4-6831-477E-A5F7-F792327ED8F6&IDImmobile=0&runtimeId=gxAnnuncio-1636898004488338465&pathName=%2Fen%2Fproperty%2Fresidential%2Frent%2Fapartment%2Ffirenze-fi%2F" + Id
            for Id in Ids]
        Ids = [
            Id + '&clientId=&clientClassName=&lingua=en&urlPaginaAnnuncio=%2Fen%2Fproperty%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fen%2Fconstruction%2F%7Bidcantiere%7D&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=true&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=false&mostraIconaAsta=false&mostraGallery=true&carousel=true&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true&mostraPrivacy=true&urlPrivacy=%2Fen%2Fprivacy&_=1636898004248'
            for Id in Ids]

        for link in Ids:
            yield Request(url=link, callback=self.parse_area, method = 'GET')

    def parse_area(self, response):
        item_loader = ListingLoader(response=response)

        html = response.text
        html = html.strip()
        html = html.replace(" ", "")
        html = html.replace("\\", "")
        html = html.replace("\\t", "")
        html = html.replace("\\n", "")
        html = html.replace("\\r", "")
        html = html.replace("&gt;", ">")
        html = html.replace("&lt;", "<")

        link = re.findall('gxAnnuncioIDImmobile"value="([0-9]+)', html)
        link = list(dict.fromkeys(link))
        external_link = 'https://www.gierreimmobiliare.it/en/property/residential/rent/apartment/firenze-fi/' + link[0]
        external_id = re.findall('</span><span>(\w+/\w+)</span></li></ul>', html)
        external_source = self.external_source

        html = response.text
        html = html.strip()
        html = html.replace("\\", "")
        html = html.replace("\\t", "")
        html = html.replace("\\n", "")
        html = html.replace("\\r", "")
        html = html.replace("&gt;", ">")
        html = html.replace("&lt;", "<")
        title =re.findall('<h1>((?:[\w]+|\s)+)<\/h1><', html)
        description = (re.findall('<p class="gx-printable">([\w\s\.,-:+€\(\)\"!]+)<\/p>', html))
        latitude = (re.findall('data-lat="(\d+.\d+)"', html))[0]
        longitude = (re.findall('data-lng="(\d+.\d+)"', html))[0]
        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']
        property_type = 'apartment'
        square_meters = int((re.findall('<span>(\d*)\ssqm</span>', html))[0])
        features_keys = (re.findall('<label class="gx-label-details">([\w\s]+)', html))
        features_values = (re.findall('<span class="gx-val-details">([\w|\s|-|/|€,]+)', html))
        zip_iterator = zip(features_keys, features_values)
        features = dict(zip_iterator)
        room_count = features.get('no bedrooms')
        if room_count is None:
            room_count = 1
        bathroom_count = features.get('no bathrooms')
        images = re.findall('style="background-image:url\(\'(https:\/\/[\w\.\/-]+\/xl\.jpg)', html)
        images = list(dict.fromkeys(images))
        external_images_count = len(images)
        rent = int(extract_number_only(re.findall('<strong>€\s(\d+.\d+)',html)))
        currency = "EUR"

        utilities = extract_number_only(extract_number_only(features.get('expense')))
        if (utilities != '') and (utilities!='0'):
            utilities = int(float(utilities[:-2]))
        energy_label = features.get('energy class')


        if "furni" in description:
            furnished = True
        else:
            furnished = False

        if 'floor' in features.keys():
            floor = features.get('floor')
        else:
            floor = "1"

        if features.get('garage') == 'absent':
            parking = False
        else:
            parking = True

        if features.get('no elevators'):
            elevator = True
        else:
            elevator = False


        if features.get('no terraces'):
            terrace = True
        else:
            terrace = False

        if "washing" in description:
            washing_machine = True
        else:
            washing_machine = False

        if "dish" in description:
            dishwasher = True
        else:
            dishwasher = False

        # --------------------------------#
        # item loaders
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_source', external_source)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('address', address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', square_meters)
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('bathroom_count', bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("floor", floor)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("landlord_name", 'gierre immobiliare')
        item_loader.add_value("landlord_email", 'info@gierreimmobiliare.it')
        item_loader.add_value("landlord_phone", '055292355')

        yield item_loader.load_item()
