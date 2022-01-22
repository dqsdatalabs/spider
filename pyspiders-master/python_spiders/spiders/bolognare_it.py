# -*- coding: utf-8 -*-
from functools import partial
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class BolognareItSpider(scrapy.Spider):
    name = 'bolognare_it'
    allowed_domains = ['bolognare.it']
    start_urls = ['https://www.bolognare.it/properties/?realteo_order=&_offer_type=rent&tax-tipo_immobile%5B%5D=appartamento&_area_min=&_area_max=&_price_min=&_price_max=']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("div.listings-container.compact>div>div"):
            try:
                url = appartment.css(
                    "div.listing-item.compact>a.listing-img-container").attrib['href']
                yield Request(url,
                              callback=self.populate_item,
                              dont_filter=True,
                              )
            except:
                pass

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('div.property-title>h2::text').get().strip()
        address = response.css(
            '#titlebar > div > div > div > div.property-title > span > a::text').extract()[1].strip()

        external_id = response.css(
            'div.property-pricing>div.sub-price::text').extract()[1].strip().split("Rif.: ")[1]

        rent = response.css(
            'div.property-pricing>div::text').get().split("€")[0].strip()

        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0] + rent_array[1]

        description = response.css(
            "div.property-description.print-only > p::text").get()

        images = response.css(
            'div.property-slider-nav>div.item>img::attr(src)').extract()

        space = response.css("li.main-detail-_area>span::text").get()
        rooms = response.css("li.main-detail-_rooms>span::text").get()
        bathrooms = response.css("li.main-detail-_bathrooms>span::text").get()

        features = response.css("ul.property-features.margin-top-0>li")

        floor = None
        parking = None
        utility = None
        furnished = None
        for item in features:
            if "Piano:" in item.css('li::text').get():
                floor = item.css("span::text").get().strip()
            elif "Garage:" in item.css('li::text').get():
                parking = item.css("span::text").get().strip()
                if "No" in parking:
                    parking = False
                else:
                    parking = True
            elif "Arredamento:" in item.css('li::text').get():
                furnished = item.css("span::text").get().strip()
                if "Arredato" in furnished:
                    furnished = True
                else:
                    furnished = False
            elif "Spese Condominiali:" in item.css('li::text').get():
                data = item.css("span::text").get().strip()
                utility = data.split(' ')[1]

        lat = response.css("#propertyMap::attr(data-latitude)").get()
        long = response.css("#propertyMap::attr(data-longitude)").get()

        landlord_name = response.css(
            "#widget_contact_widget_findeo-2 > div > div.agent-title > div.agent-details > h4 > a::text").get()

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
        item_loader.add_value("city", "Bologna")
        item_loader.add_value("floor", floor)
        item_loader.add_value("furnished", furnished)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", long)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("utilities", utility)
        item_loader.add_value("currency", "EUR")

        # House Rules
        item_loader.add_value("parking", parking)

        # LandLord Details
        item_loader.add_value("landlord_phone", "0514399031")
        item_loader.add_value(
            "landlord_email", "info@agenziacostasaragozza.it")
        item_loader.add_value("landlord_name", landlord_name)

        yield item_loader.load_item()
