# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class SitoErrebicasaNetSpider(scrapy.Spider):
    name = 'mlproperties_it'
    allowed_domains = ['mlproperties.it']
    start_urls = [
        'https://mlproperties.it/risultato-ricerca/?country%5B%5D=&location%5B%5D=&status%5B%5D=affitto&label%5B%5D=&min-price=&max-price=']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#main-wrap > section.listing-wrap.listing-v1 > div > div.row > div > div.listing-view.list-view.card-deck>div"):
            yield Request(appartment.css("a.listing-featured-thumb.hover-effect").attrib['href'],
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            '#main-wrap > section > div.page-title-wrap > div > div.d-flex.align-items-center.property-title-price-wrap > div > h1::text').get().strip()
        address = response.css(
            'address.item-address::text').extract()[1].strip()
        city = address.split(",")[1].strip()
        zipcode = response.css(
            '#property-address-wrap > div > div.block-content-wrap > ul > li.detail-zip > span::text').get()

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        rent = response.css(
            'li.item-price::text').get().split("€")[0].strip()

        if "," in rent:
            rent_array = rent.split(",")
            rent = rent_array[0] + rent_array[1]

        description = ''
        description_array = response.css(
            "#property-description-wrap > div > div.block-content-wrap > p::text").extract()

        for text in description_array:
            description += text

        images = response.css(
            'img.img-fluid.lazyload::attr(data-src)').extract()

        space = response.css(
            "#main-wrap > section > div.property-top-wrap > div > div.container.hidden-on-mobile > div > div.col-md-12 > div > div > ul:nth-child(4) > li.property-overview-item > strong::text").get()
        if space == "0" or space == None:
            return

        rooms = response.css(
            "#main-wrap > section > div.property-top-wrap > div > div.container.hidden-on-mobile > div > div.col-md-12 > div > div > ul:nth-child(2) > li.property-overview-item > strong::text").get()
        bathrooms = response.css(
            "#main-wrap > section > div.property-top-wrap > div > div.container.hidden-on-mobile > div > div.col-md-12 > div > div > ul:nth-child(3) > li.property-overview-item > strong::text").get()

        coords = response.xpath(
            '//*[@id="houzez-single-property-map-js-extra"]/text()').get()
        lat = coords.split('"lat":"')[1].split('"')[0]
        long = coords.split('"lng":"')[1].split('"')[0]

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
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", long)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "390236567499")
        item_loader.add_value("landlord_email", "info@mlproperties.it")
        item_loader.add_value("landlord_name", "ML properties")

        yield item_loader.load_item()
