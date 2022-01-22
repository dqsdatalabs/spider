import scrapy
from scrapy import Request
from scrapy.selector import Selector

from ..loaders import ListingLoader
from ..helper import *

import re
import requests
from urllib.parse import urlparse, urlunparse, parse_qs

class SogesSpider(scrapy.Spider):
    name = 'soges_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Soges_PySpider_" + country + "_" + locale

    allowed_domains = ['www.soges.it']
    start_urls = [
        'https://www.soges.it/r-immobili/?Codice=&Motivazione%5B%5D=2&localita=&Regione%5B%5D=0&Provincia%5B%5D=0&Comune%5B%5D=0&Tipologia%5B%5D=1&Tipologia%5B%5D=38&Tipologia%5B%5D=2&Tipologia%5B%5D=40&Tipologia%5B%5D=26&Tipologia%5B%5D=8&Tipologia%5B%5D=41&Tipologia%5B%5D=36&Tipologia%5B%5D=3&Tipologia%5B%5D=49&Tipologia%5B%5D=9&Tipologia%5B%5D=39&Tipologia%5B%5D=10&Prezzo_a=&Locali_da=&Camere_da=&Bagni_da=&Totale_mq_da=&Totale_mq_a=&cf=yes&map_circle=0&map_polygon=0&map_zoom=0&p=0']

    position = 1

    def parse(self, response):
    
        cards = response.css(".griglia li")

        for index, card in enumerate(cards):

            position = self.position
            card_url = card.css("figure a::attr(href)").get()

            property_type = card.css(".titolo::text").get()
            if property_type:
                property_type = property_type_lookup[property_type]

            square_meters = card.css(".icone .ico:contains('mq') span::text").get()
            if square_meters:
                square_meters = square_meters.split(" ")[0]

            bathroom_count = card.css(".icone .ico:contains('Bagni') span::text").get()
            if bathroom_count:
                bathroom_count = bathroom_count.split(" ")[0]

            external_id = card.css(".action .codice::text").get()
            if external_id:
                external_id = external_id.split(" ")[1]

            rent = card.css(".prezzo::text").get()
            if rent:
                rent = rent.split(" ")[1]

            currency = card.css(".prezzo::text").get()
            if currency:
                currency = currency_parser(currency, self.external_source)

            dataUsage = {
                "card_url": card_url,
                "position": position,
                "property_type": property_type,
                "square_meters": square_meters,
                "bathroom_count": bathroom_count,
                "external_id": external_id,
                "rent": rent,
                "currency": currency,
            }

            
            SogesSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        if len(cards) > 0:
            
            prev_page = int(parse_qs(response.url)['p'][0])
            next_page = int(parse_qs(response.url)['p'][0]) + 1
            parsed = urlparse(response.url)
            new_query = parsed.query.replace(f"&p={prev_page}",f"&p={next_page}")
            parsed = parsed._replace(query= new_query)
            nextPageUrl = urlunparse(parsed)
            
            if nextPageUrl:
                yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True)
        else:
            pass

    def parseApartment(self, response):

        room_count = response.css("#sezInformazioni .box strong:contains('Locali')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1


        script_map = response.css("#sezMappa > script:nth-child(3)::text").get()
        if script_map:
            pattern = re.compile(r'var lat = "(\d*\.?\d*)";')
            x = pattern.search(script_map)
            pattern = re.compile(r'var lgt = "(\d*\.?\d*)";')
            y = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = y.groups()[0]

        landlord_name = response.css('.dettagli a:nth-of-type(1) strong::text').get()
        landlord_email = response.css('.dettagli a:nth-of-type(2)::text').get()
        landlord_phone = response.css('.dettagli a:nth-of-type(3)::attr(href)').get().split(":")[1]

        address = remove_white_spaces(response.css('.interno-scheda h1 span::text').get()) + " - " + \
                  response.css('.interno-scheda h2::text').get().split(":")[1].strip()
        city = address.split(" - ")[0]
        street = address.split(" - ")[1]
        area = address.split(" - ")[2]

        title = response.css('head title::text').get()

        description = response.css('.testo.woww p::text, .testo.woww p strong::text').getall()
        description = " ".join(description)

        energy_label = response.css('.classe_energetica .liv_classe::text').get() or ""

        dataForImage = response.css(
            '.slFotoNow::attr(data-id), .slFotoNow::attr(data-tipo), .slFotoNow::attr(data-pagination)').getall()
        responseImages = requests.post(
            "https://www.soges.it/moduli/swiper_foto.php",
            data={
                "id_slider": dataForImage[0],
                "sezione": dataForImage[1],
                "pagination": dataForImage[2],
            })
        images = Selector(text=responseImages.text).css('.ClickZoomGalleryThumb img::attr(data-src)').getall()

        external_images_count = len(images)

        features = response.css('#sezInformazioni div.box')

        elevator = features.css(":contains('Ascensore')::text").get()
        if elevator:
            elevator = True

        floor = features.css(":contains('Piano')::text").get()
        if floor:
            floor = floor.split(":")[1]

        balcony = features.css(":contains('Balconi')::text").get()
        if balcony:
            balcony = True

        terrace = features.css(":contains('Terrazzo')::text").get()
        if terrace:
            terrace = True

        furnished = features.css(":contains('Arredato')::text").get()
        if furnished:
            furnished = True

        utilities = features.css(":contains('Spese condominio')::text").get()
        if utilities:
            utilities = utilities.split(" ")[-1]

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Soges_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_id", response.meta['external_id'])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", "")
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", response.meta['square_meters'] or 1)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", response.meta['rent'])
        item_loader.add_value("currency", response.meta['currency'])
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("floor", floor)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
        pass


def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and (
            "apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and (
            "house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number >= 92:
        energy_label = "A"
    elif energy_number >= 81 and energy_number <= 91:
        energy_label = "B"
    elif energy_number >= 69 and energy_number <= 80:
        energy_label = "C"
    elif energy_number >= 55 and energy_number <= 68:
        energy_label = "D"
    elif energy_number >= 39 and energy_number <= 54:
        energy_label = "E"
    elif energy_number >= 21 and energy_number <= 38:
        energy_label = "F"
    else:
        energy_label = "G"
    return energy_label
