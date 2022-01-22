# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class FurbattoItSpider(scrapy.Spider):
    name = 'furbatto_it'
    allowed_domains = ['furbatto.it']
    start_urls = [
        'https://www.furbatto.it/index.php/proposte/elencoProposte/cat/affitto_residenziale']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        for appartment in response.css("div.row.griglia-proposte > div"):
            yield Request(appartment.css("div.card>a").attrib['href'],
                          callback=self.populate_item
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            '#main-proposte-area > div > h1::text').get().strip()
        location = response.css("head > script:nth-child(33)::text").get()
        address = location.split('"streetAddress": "')[1].split('",')[0]
        city = location.split('"addressRegion": "')[1].split('",')[0]
        zipcode = location.split('"postalCode": "')[1].split('"')[0]

        if "MQ" not in title or "€" not in title:
            return

        space = title.split("MQ ")[1].split(" - ")[0]

        rent = title.split("€ ")[1].split(" ")[0]

        if "/" in rent:
            rent = rent.split("/")[0]

        description = response.css(
            "p.prop-description::text").get()

        images = response.css(
            'li::attr(data-thumb)').extract()

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "011544566")
        item_loader.add_value("landlord_email", "contacts@furbatto.it")
        item_loader.add_value("landlord_name", "Furbatto Immobili")

        yield item_loader.load_item()
