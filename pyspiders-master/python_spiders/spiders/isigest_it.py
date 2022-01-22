# -*- coding: utf-8 -*-
# Author: Mahmoud Wessam
import scrapy
from ..loaders import ListingLoader
from scrapy.http.request import Request


class IsigestItSpider(scrapy.Spider):
    name = "isigest_it"
    start_urls = ['https://www.isigest.it/immobili/index?code=&scopo_immobile=residenziale&scopo_tipologia=&contratto=affitto&zona1=&mq_comm=0.00-500.00&prezzo=0-1600']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for appartment in response.css("#listato_proposte > li"):
            url = "https://www.isigest.it" + \
                appartment.css("div.descr_immobile > h4 > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('#titolo::text').get()
        if 'Courmayeur' in title:
            city = 'Courmayeur'
        elif 'Saint Pierre' in title:
            city = 'Saint Pierre'
        elif 'La Salle' in title:
            city = 'La Salle'


        external_id = response.css(
            '#codice-riferimento::text').get().replace("Rif. ", "")

        rent = response.css('#prezzo-immobile::text').get()

        if "," not in rent:
            return
        else:
            rent = rent.split(',')[0].split("€")[1].strip()

        images = response.css('a.effetto_lightbox::attr(href)').extract()

        description = response.css(
            '#dettaglio > section.descr_immobile > p::text').extract()

        dishwasher = None
        washing_machine = None
        energy_label = None
        for item in description:
            if "lavastoviglie" in item:
                dishwasher = True
            if "lavatrice" in item:
                washing_machine = True
            if "Classe energetica " in item:
                energy_label = item.split('Classe energetica ')[1][0]

        ameneties = response.css(
            'div.tabella-riassuntiva > div > div')

        square_meters = None
        floor = None
        bathroom_count = None
        room_count = None
        parking = None
        terrace = None
        balcony = None
        property_type = None
        for item in ameneties:
            if "SUPERFICIE COMMERCIALE" in item.css('strong::text').get():
                square_meters = item.css(
                    'span::text').get().split(' mq')[0]
            elif "PIANO" in item.css('strong::text').get():
                if item.css('span'):
                    floor = item.css('span::text').get()
                else:
                    floor = item.css('p::text').get()
            elif "BAGNI" in item.css('strong::text').get():
                if "Due" in item.css('p::text').get():
                    bathroom_count = 2
                else:
                    bathroom_count = 1
            elif "VANI" in item.css('strong::text').get():
                room_count = item.css('span::text').get()[-1]
                try:
                    int(room_count)
                except:
                    room_count = item.css('span::text').get().split(',')[
                        1].strip()
                    room_count = room_count[0]
            elif "SPAZI ESTERNI" in item.css('strong::text').get():
                if "Giardino" in item.css('p::text').get():
                    parking = True
                if "terrazza" in item.css('p::text').get():
                    terrace = True
                if "balconi" in item.css('p::text').get():
                    balcony = True
            elif "TIPOLOGIA" in item.css('strong::text').get():
                if "trilocale" in item.css('p::text').get() or "apartment" in item.css('p::text').get():
                    property_type = 'apartment'
                elif "attico" in item.css('p::text').get():
                    property_type = 'room'
                elif "monolocale" in item.css('p::text').get():
                    property_type = 'studio'

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value(
            "external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city) # String
        # item_loader.add_value("zipcode", zipcode) # String
        # item_loader.add_value("address", address) # String
        # item_loader.add_value("latitude", latitude) # String
        # item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", property_type)  # String
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Isigest Immobili')  # String
        item_loader.add_value("landlord_phone", '0165 800093')  # String
        item_loader.add_value(
            "landlord_email", 'segreteria.isigest@gmail.com')  # String

        self.position += 1
        yield item_loader.load_item()
