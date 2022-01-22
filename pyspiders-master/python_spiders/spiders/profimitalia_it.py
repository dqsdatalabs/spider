# -*- coding: utf-8 -*-
import re
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from scrapy import FormRequest


class ProfimitaliaItSpider(scrapy.Spider):
    name = 'profimitalia_it'
    allowed_domains = ['profimitalia.it']
    start_urls = [
        'https://profimitalia.it/elenco_immobili_f.asp?rel=nofollow']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def start_requests(self):
        formdata = {
            "idcau": "2",
        }

        yield FormRequest(
            url=self.start_urls[0],
            callback=self.parse,
            formdata=formdata,
        )

    def parse(self, response):
        for appartment in response.css("div.span8>div.row"):
            url = appartment.css('a.span4.overlay').attrib['href']

            cost_and_space = appartment.css(
                "span.qty.pull-right::text").extract()
            cost = cost_and_space[2].strip().split(" ")[0]
            space = cost_and_space[1].strip().split(" ")[0]

            yield Request("https://profimitalia.it/"+url,
                          callback=self.populate_item,
                          dont_filter=False,
                          meta={"cost": cost,
                                "space": space}
                          )
        try:
            next_page = response.css('a[rel="Next"]').attrib['href']
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            'h2.prop-title.pull-left.margin0::text').get().strip()

        address = title.split(" A ")[1]
        city = address.split("  ")[0]

        rent = response.meta["cost"].strip()
        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0] + rent_array[1]

        description = response.css(
            "div.clearfix.padding30>p::text").get().split("TEL ")[0]

        features_left = response.css(
            'div.clearfix.padding030.row-fluid.marginBottom30>ul.more-info.pull-left.span6>li')

        features_right = response.css(
            '#red > div.container.page-content > div > div.span8 > div > div.clearfix > div:nth-child(2) > ul.more-info.pull-right.span6>li')

        rooms = None
        floor = None
        elevator = None
        balcony = None
        parking = None
        for item in features_left:
            if "Vani:" in item.css('li>span.pull-left::text').get():
                rooms = item.css('li>span.qty.pull-right::text').get()
            elif "Piano:" in item.css('li>span.pull-left::text').get():
                floor = item.css('li>span.qty.pull-right::text').get()
            elif "Balcone:" in item.css('li>span.pull-left::text').get():
                if "SI" in item.css('li>span.qty.pull-right::text').get():
                    balcony = True
                else:
                    balcony = False

        for element in features_right:
            if element.css('li>span.pull-left::text').get() is not None:
                if "Auto:" in element.css('li>span.pull-left::text').get():
                    if "nessuno" not in element.css('li>span.qty.pull-right::text').get():
                        parking = True
                    else:
                        parking = False
                elif "Ascensore:" in element.css('li>span.pull-left::text').get():
                    if "NO" in element.css('li>span.qty.pull-right::text').get():
                        elevator = False
                    else:
                        elevator = True

        images = response.css(
            'ul.thumb-list>li>a>img::attr(src)').extract()

        lat = None
        lng = None
        try:
            coords = response.css(
                "head > script:nth-child(28)::text").get() .split(".LatLng(")[1].split(");")[0]

            lat = coords.split(", ")[0]
            lng = coords.split(", ")[1]
        except:
            pass

        # # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("=")[-1])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", 'apartment')
        item_loader.add_value("square_meters", int(
            response.meta["space"].strip()))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)

        item_loader.add_value("latitude", lat.replace("'", ""))
        item_loader.add_value("longitude", lng.replace("'", ""))

        item_loader.add_value("floor", floor)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("parking", parking)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "010591323")
        item_loader.add_value("landlord_email", "info@profimitalia.it")
        item_loader.add_value("landlord_name", "Profimitalia")

        yield item_loader.load_item()
