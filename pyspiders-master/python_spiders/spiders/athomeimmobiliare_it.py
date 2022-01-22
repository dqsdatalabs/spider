# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class AthomeimmobiliareItSpider(scrapy.Spider):
    name = 'athomeimmobiliare_it'
    allowed_domains = ['athomeimmobiliare.it']
    start_urls = ['https://www.athomeimmobiliare.it/advanced-search/?advanced_city=&filter_search_action%5B%5D=affitto&filter_search_type%5B%5D=residenziale&cerca-per-id=']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#listing_ajax_container"):
            yield Request(appartment.css("h4>a").attrib['href'],
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            '#all_wrapper > div > div.container.content_wrapper > div > div.col-md-12.full_width_prop > h1::text').get().strip()

        rent = response.css(
            '#all_wrapper > div > div.container.content_wrapper > div > div.col-md-12.full_width_prop > span.price_area::text').get().strip().split(" ")[1].strip()

        description_array = response.css(
            "div.wpestate_property_description>p::text").extract()

        description = ""
        for item in description_array:
            if "www" in item or "numeri" in item or item is None:
                pass
            description += item

        images = response.css('a.prettygalery>img::attr(src)').extract()

        features = response.css("div.listing_detail.col-md-4")

        floor = None
        space = None
        rooms = None
        bathrooms = None
        external_id = None
        elevator = None
        address = None
        city = None
        zipcode = None
        try:
            for item in features:
                if "Indirizzo:" in item.css('strong::text').get():
                    address = item.css("div::text").get().strip()
                elif "Città:" in item.css('strong::text').get():
                    city = item.css("a::text").get().strip()
                elif "Cap:" in item.css('strong::text').get():
                    zipcode = item.css("div::text").get().strip()
                elif "ID proprietà:" in item.css('strong::text').get():
                    external_id = item.css("div::text").get().strip()
                elif "Bagni:" in item.css('strong::text').get():
                    bathrooms = item.css("div::text").get().strip()
                elif "Camere:" in item.css('strong::text').get():
                    rooms = item.css("div::text").get().strip()
                elif "Ascensore:" in item.css('strong::text').get():
                    elevator = item.css("div::text").get().strip()
                    if "no" in elevator:
                        elevator = False
                    else:
                        elevator = True
                elif "Dimensioni della proprietà:" in item.css('strong::text').get():
                    space = item.css("div::text").get().strip().split(" ")[0]
        except:
            pass

        lat = response.css("#googleMapSlider::attr(data-cur_lat)").get()
        lng = response.css("#googleMapSlider::attr(data-cur_long)").get()

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
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("floor", floor)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "095553453")
        item_loader.add_value("landlord_email", "info@athomeimmobiliare.it")
        item_loader.add_value("landlord_name", "At-Home Real Estate")

        yield item_loader.load_item()
