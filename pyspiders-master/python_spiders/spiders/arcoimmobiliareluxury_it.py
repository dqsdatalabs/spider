# -*- coding: utf-8 -*-
# Author: Abanoub Moris

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.helper import remove_white_spaces
from python_spiders.loaders import ListingLoader
import json
import re
import requests


class MySpider(Spider):
    name = 'arcoimmobiliareluxury'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    pos = 1

    def start_requests(self):

        url = "https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=EE6F815C-D820-45E6-A75C-F95195729439&querystring=?CodiceNazione=IT&Contratto=A&Categoria=1&StartRisultati=0&clientId=&clientClassName=&numeroColonne=2&lingua=it&numeroRisultati=10&urlPaginaRisultati=/it/annunci-immobiliari&urlPaginaAnnuncio=/it/immobile/{categoria}/{contratto}/{tipologia}/{comune}-{siglaprovincia}/{idimmobile}&urlPaginaCantiere=/it/cantiere/{idcantiere}"

        yield Request(
            url=url,
            callback=self.parse)

    def parse(self,response):

        for page in range(0,100,10):
            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=EE6F815C-D820-45E6-A75C-F95195729439&querystring=?CodiceNazione=IT&Contratto=A&Categoria=1&StartRisultati={page}" + \
                "&clientId=&clientClassName=&numeroColonne=2&lingua=it&numeroRisultati=10&urlPaginaRisultati=/it/annunci-immobiliari&urlPaginaAnnuncio=/it/immobile/{categoria}/{contratto}/{tipologia}/{comune}-{siglaprovincia}/{idimmobile}&urlPaginaCantiere=/it/cantiere/{idcantiere}"
            yield Request(url, callback=self.parseApartment)

        
    # 1. FOLLOWING
    def parseApartment(self, response):
        data = ""
        try:
            data = json.loads(response.body)
            data = json.loads(data)["html"]
        except:
            data = response.body.split(b">")[1].split(b"<")[0].strip()
            data = json.loads(data)["html"].replace(
                "&lt;", "<").replace("&gt;", ">")

        sel = Selector(text=data, type="html")

        for item in sel.css('.gx-risultato'):

            title = item.css('h2::text').get()
            description = "".join(item.xpath(
                ".//div[@class='gx-risultato-description']//text()").getall())

            
            external_id = item.xpath("./a/@href").get()

            external_link = 'https://www.arcoimmobiliareluxury.it'+external_id
            rent = 0
            try:
                rent = item.css('.gx-risultato .gx-prezzo *:contains(€)::text').get().replace(
                    '.', '').replace(' ', '').replace('€', '')
            except:
                rent = 0
            prop_type = item.xpath(".//h2/text()").get()
            address = item.xpath(
                ".//div[@class='gx-risultato-testo']/h3/text()").get()

            room_count = item.xpath(
                ".//span[contains(.,'Local')]/following-sibling::span/text()").get()
            if not room_count:
                room_count = '1'
            square_meters = item.xpath(
                ".//span[contains(.,'Superficie')]/following-sibling::span/text()").get()

            if get_p_type_string(prop_type):
                pass
            else:
                continue
            if external_id:
                external_id = external_id.split("/")[-1]

            # ********************

            dataUsage = {
                "property_type": get_p_type_string(prop_type),
                'description': re.sub('\s{2,}', ' ', remove_white_spaces(description.strip())),
                'title': title,
                "external_id": external_id,
                "external_link": external_link,
                "city": "Rome",
                "address": address,
                "square_meters": square_meters.split("m")[0].strip(),
                "room_count": room_count,
                "rent": rent
            }

            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?apikey=EE6F815C-D820-45E6-A75C-F95195729439&IDImmobile={external_id}&runtimeId=gxAnnuncio-163750420288984501&pathName=%2F&clientId=&clientClassName=&lingua=it&urlPaginaAnnuncio=&urlPaginaCantiere=undefined&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=false&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=true&mostraGallery=true&carousel=false&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true&mostraPrivacy=true&urlPrivacy=/it/privacy&_=1637504202354"

            yield Request(url, callback=self.get_images, meta=dataUsage)
           
    def get_images(self, response):

        property_type = response.meta['property_type']
        title = response.meta['title']
        external_id = response.meta['external_id']
        external_link = response.meta['external_link'].replace('https', 'http')
        city = response.meta['city']
        if not response.meta['address']:
            return
        address = response.meta['address']
        square_meters = response.meta['square_meters']
        room_count = response.meta['room_count']
        rent = response.meta['rent']
        description = response.meta['description']
        description = re.sub(r'[\w]+ info.+',"",description)

        if int(rent) == 0 or int(rent) > 20000:
            return

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        address = responseGeocodeData['address']['Match_addr']

        longitude = str(longitude)
        latitude = str(latitude)

        data = response.body.split(b">")[1].split(b"<")[0].strip()
        data = json.loads(data)["html"].replace(
            "&lt;", "<").replace("&gt;", ">")
        sel = Selector(text=data, type="html")
        images = [x.split("(")[1].split(")")[0] for x in sel.xpath(
            "//div[@class='gx-gallery-slide']//@style").getall()]
        bathroom_count = sel.xpath(
            "//ul[@class='gx-labs-list-inline']//li[contains(.,'bagni')]//text()").get()

        energy_label = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'classe energetica')][1]//following-sibling::span)[1]//text()").get()

        floor = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'piano')][1]//following-sibling::span)[1]//text()").get()

        if not floor or not floor.isdigit():
            floor = ''

        parking = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'posti auto')]//following-sibling::span)[1]//text()").get()

        elevator = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'scensori')]//following-sibling::span)[1]//text()").get()

        utilities = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'spese')]//following-sibling::span)[1]//text()").get()
        if utilities:
            utilities = utilities.replace(
                ',00', '').replace('€', '').replace(' ', '')

        balcony = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'balconi')]//following-sibling::span)[1]//text()").get()

        terrace = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'terrazzi')]//following-sibling::span)[1]//text()").get()

        if int(rent) < 20000:
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", external_link)  # String
            item_loader.add_value(
                "external_source", self.external_source)  # String
            item_loader.add_value(
                "external_id", external_id)  # String
            item_loader.add_value("position", self.pos)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", latitude)  # String
            item_loader.add_value("longitude", longitude)  # String
            if floor:
                item_loader.add_value("floor", floor)
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            if bathroom_count:
                item_loader.add_value(
                    "bathroom_count", bathroom_count.split("bagni")[0])

            # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

            # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            if parking:
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
            if elevator:
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
            if balcony and "none" not in balcony.lower():
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
            if terrace and "none" not in terrace.lower():
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)
            # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            # item_loader.add_value("washing_machine", washing_machine) # Boolean
            # item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            if energy_label:
                item_loader.add_value("energy_label", energy_label)

            # LandLord Details
            item_loader.add_value("landlord_name", "ARCO IMMOBILIARE LUXURY")
            item_loader.add_value("landlord_phone", "3332840145")
            item_loader.add_value(
                "landlord_email", 'arcoimmobiliaresanastasia@gmail.com')

            self.pos += 1
            yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None
