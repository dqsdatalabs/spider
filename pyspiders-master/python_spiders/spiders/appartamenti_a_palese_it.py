# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class AppartamentiAPaleseItSpider(scrapy.Spider):
    name = 'appartamenti-a-palese_it'
    allowed_domains = ['appartamenti-a-palese.it']
    start_urls = [
        'https://www.appartamenti-a-palese.it/property-search/?status=affitto']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        for appartment in response.css("body > div.rh_wrap > section.rh_section.rh_section--flex.rh_section__map_listing > div.rh_page.rh_page__map_properties > div.rh_page__listing > article"):
            yield Request(appartment.css("h3>a").attrib['href'],
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            'h1.rh_page__title::text').get()

        address = response.css(
            'p.rh_page__property_address::text').get().strip()
        try:
            city_and_zip = address.split(", ")[1].strip()
            city = city_and_zip.split(" ")[1]
            zip = city_and_zip.split(" ")[0]
        except:
            pass

        rent = response.css(
            'p.price::text').get().strip().split(" ")[0].split("€")[1].strip()

        if "," in rent:
            rent = rent.split(",")[0]

        description = response.css(
            "body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_property > div.rh_property__wrap.rh_property--padding > div.rh_property__main > div > div.rh_content > p::text").extract()

        images = response.css(
            'a.swipebox::attr(href)').extract()

        features = response.css(
            "body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_property > div.rh_property__wrap.rh_property--padding > div.rh_property__main > div > div.rh_property__row.rh_property__meta_wrap>div")

        # rooms = None
        bathrooms = None
        space = None
        for item in features:
            if "Area" in item.css('h4::text').get():
                space = item.css(
                    "span.figure::text").get().strip()
            elif "Bagni" in item.css('h4::text').get():
                bathrooms = item.css(
                    "span.figure::text").get().strip()

        coords = response.xpath("/html/body/script[9]/text()").get()
        lat = coords.split('"lat":"')[1].split('",')[0]
        lng = coords.split('"lng":"')[1].split('",')[0]

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        # item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zip)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "3357767249")
        item_loader.add_value("landlord_email", "direcasainfo@gmail.com")
        item_loader.add_value("landlord_name", "Agent Dire Casa")

        yield item_loader.load_item()
