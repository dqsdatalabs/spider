# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class DgreggioimmobiliareItSpider(scrapy.Spider):
    name = 'dgreggioimmobiliare_it'
    allowed_domains = ['dgreggioimmobiliare.it']
    start_urls = [
        'https://www.dgreggioimmobiliare.it/ricerca-immobili.html?tipologia=Affitto%7C0&categoria=11&comune=&vani=0&mqda=&mqfinoa=&prezzo_a=0']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#properties>div"):
            yield Request(appartment.css(
                "div.info>header>a").attrib['href'],
                callback=self.populate_item,
                dont_filter=True,
            )

     # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            '#property-detail > header > h1::text').get().strip()
        external_id = response.xpath(
            '//*[@id="property-detail"]/header/figure/text()[5]').get().strip().split("Rif. ")[1]

        description = ''
        description_array = response.css(
            "#description > p::text").extract()

        for text in description_array:
            description += text

        images = response.css('img.img-responsive::attr(src)').extract()

        features = response.css("#quick-summary > dl>dt")
        values = response.css("#quick-summary > dl>dd")

        rent = None
        space = None
        rooms = None
        city = None
        bathrooms = None
        address = None
        utility = None
        deposit = None
        for i in range(len(features)):
            try:
                if "Superficie" in features[i].css("dt::text").get():
                    space = values[i].css("dd::text").get(
                    ).strip().split(" ")[0].strip()
                elif "Prezzo" in features[i].css("dt::text").get():
                    rent = values[i].css(
                        "dd>span::text").get().strip().split(" ")[-1]
                    if "." in rent:
                        rent_array = rent.split(".")
                        rent = rent_array[0] + rent_array[1]
                elif "Camere" in features[i].css("dt::text").get():
                    rooms = values[i].css("dd::text").get().strip()
                elif "Bagni" in features[i].css("dt::text").get():
                    bathrooms = values[i].css("dd::text").get().strip()
                elif "Località" in features[i].css("dt::text").get():
                    address = values[i].css("dd::text").get().strip()
                elif "Provincia" in features[i].css("dt::text").get():
                    city = values[i].css("dd::text").get().strip()
                elif "Condominio mensile" in features[i].css("dt::text").get():
                    utility = values[i].css("dd::text").get(
                    ).strip().split(" - ")[0].split("€ ")[1]
                elif "Deposito" in features[i].css("dt::text").get():
                    deposit = values[i].css(
                        "dd::text").get().strip().split("€ ")[1]
                    if "." in deposit:
                        deposit_array = deposit.split(".")
                        deposit = deposit_array[0] + deposit_array[1]
            except:
                pass

        coords = response.xpath(
            '//*[@id="property-mappa"]/script/text()').get()
        coords = coords.split(".setLngLat([")[1].split("])")[0]

        try:
            lat = coords.split(", ")[0]
            lng = coords.split(", ")[1]
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

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("deposit", int(deposit))
        item_loader.add_value("utilities", utility)
        item_loader.add_value("currency", "EUR")

        # # LandLord Details
        item_loader.add_value("landlord_phone", "390259903998")
        item_loader.add_value("landlord_email", "info@dgreggioimmobiliare.it")
        item_loader.add_value("landlord_name", "Greggio Immobiliare")

        yield item_loader.load_item()
