# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class ImmobiliarecasamiaNetSpider(scrapy.Spider):
    name = 'immobiliarecasamia_net'
    allowed_domains = ['immobiliarecasamia.net']
    start_urls = ['https://www.immobiliarecasamia.net/ita/immobili?order_by=%5B%22field%22%2C%22company_id%22%2C%2239%22%5D%3Bdate_update_desc&rental=1&property_type_id=1001&city_id=&price_max=&size_min=&size_max=&page=1']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        next_page = "https://www.immobiliarecasamia.net/immobili?order_by=%5B%22field%22%2C%22company_id%22%2C%2239%22%5D%3Bdate_update_desc&rental=1&property_type_id=1001&city_id=&price_max=&size_min=&size_max=&page={}"
        i = 1
        while i <= len(response.css("#immobili-elenco>div.row>div:nth-child(1)>select>option")):
            yield Request(next_page.format(i), callback=self.follower)
            i = i+1

    def follower(self, response):
        for appartment in response.css("#immobili-elenco>div.card"):
            url = appartment.css(
                "div.card>a.foto").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          dont_filter=True,
                          )

    # 2. SCRAPING level 2

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('div.title>span.type::text').get().strip()
        address = response.css(
            '#main_content > div.heading > div > div > span.location::text').get().strip()
        city = address.split(",")[0]

        rent = response.css(
            '#main_content > div.heading > p::text').get().strip().split(" ")[1]

        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0] + rent_array[1]

        external_id = response.css(
            '#main_content > div.heading > p>span::text').get().strip().split("Rif.")[1].strip()

        description = response.css("#content1 > p::text").get()

        images = response.css('img.sl::attr(src)').extract()

        features = response.css("#content2>div>ul>li")

        rooms = None
        space = None
        parking = None
        bathrooms = None
        floor = None
        utils = None
        energy = None
        elevator = None
        balcony = None
        for item in features:
            try:
                if "Locali" in item.css("span::text").get():
                    rooms = item.css("b::text").get().strip()
                elif "MQ" in item.css("span::text").get():
                    space = item.css("b::text").get().strip()
                elif "Parcheggio (Posti Auto)" in item.css("span::text").get():
                    parking = True
                elif "Bagni" in item.css("span::text").get():
                    bathrooms = item.css("b::text").get().strip()
                elif "Piano" in item.css("span::text").get():
                    floor = item.css("b::text").get().strip()
                elif "Classe Energ." in item.css("span::text").get():
                    energy = item.css("b::text").get().strip()
                    if len(energy) > 2 or "VA" in energy:
                        pass
                elif "Spese Annuali" in item.css("span::text").get():
                    utils = item.css(
                        "b::text").get().strip().split(' ')[1]
                    if "." in utils:
                        utils_array = utils.split('.')
                        utils = utils_array[0] + utils_array[1]
                    utils = int(int(utils)/12)
                elif "Ascensore" in item.css("span::text").get():
                    elevator = True
                elif "Balcone/i" in item.css("span::text").get():
                    balcony = True
            except:
                pass

        lat = None
        long = None
        try:
            coords = response.xpath(
                '//*[@id="tab-map"]/script[2]/text()').get().split('.LatLng(')[1].split(');')[0]
            lat = coords.split(",")[0]
            long = coords.split(",")[1]
        except:
            pass

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)

        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("parking", parking)
        item_loader.add_value("floor", floor)
        item_loader.add_value("energy_label", energy)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", long)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("utilities", utils)
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "390106001998")
        item_loader.add_value("landlord_email", "info@immobiliarecasamia.net")
        item_loader.add_value("landlord_name", "Casamia Immobiliare")

        yield item_loader.load_item()
