# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class HomezonSpider(scrapy.Spider):
    name = 'Homezon'
    allowed_domains = ['homezon.it']
    start_urls = [
        'https://www.homezon.it/immobili/immobili-su-mappa/?ofa=2&transaction_y%5b%5d=36&location-ajax-1x_y=']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#wrapper > section.section-light.section-top-shadow > div > div > div.col-xs-12.col-md-9 > div.row.list-offer-row > div.col-xs-12 > div.list-offer.rotation"):
            yield Request(appartment.css("a.list-offer-right").attrib['href'],
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            'div.details-title.pull-left > h3::text').get()

        address = response.css(
            'div.desc-parameters-val::text').extract()[-2].strip()

        if "," in address:
            address_array = address.split(",")
            address_text = ""
            for item in address_array:
                address_text += item.strip() + " "
            address = address_text.strip()

        rent = response.css(
            '#wrapper > section.section-light.no-bottom-padding > div > div > div.col-xs-12.col-md-9 > div:nth-child(1) > div.col-xs-12.col-sm-5.col-md-4 > div.details-parameters-price::text').get().split("/")[0].strip().split("€")[1].strip()

        description_array = response.css(
            "div.details-desc>p::text").extract()

        description = ""
        furnished = None
        for item in description_array:
            if "http" in item or "Telefono" in item or item is None:
                pass
            description += item
            if "Completamente arredato" in item:
                furnished = True

        images = response.css(
            'img.slide-thumb::attr(src)').extract()

        space = response.css(
            "#wrapper > section.section-light.no-bottom-padding > div > div > div.col-xs-12.col-md-9 > div:nth-child(1) > div.col-xs-12.col-sm-5.col-md-4 > div.details-parameters > div:nth-child(1) > div.details-parameters-val::text").get().strip().split(" ")[0].strip()

        features = response.css(
            "#wrapper > section.section-light.no-bottom-padding > div > div > div.col-xs-12.col-md-9 > div:nth-child(1) > div.col-xs-12.col-sm-5.col-md-4 > div.details-parameters>div")

        rooms = None
        bathrooms = None
        for item in features:
            if "Stanze" in item.css('div.details-parameters-name::text').get():
                rooms = item.css(
                    "div.details-parameters-val::text").get().strip()
            if "Bagni" in item.css('div.details-parameters-name::text').get():
                bathrooms = item.css(
                    "div.details-parameters-val::text").get().strip()

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", "Torino")
        item_loader.add_value("furnished", furnished)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "3901119837869")
        item_loader.add_value("landlord_email", "santino@homezon.it")
        item_loader.add_value("landlord_name", "Santino Balistreri ")

        yield item_loader.load_item()
