# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class LesiteimmoComSpider(scrapy.Spider):
    name = 'lesiteimmo_com'
    allowed_domains = ['lesiteimmo.com']
    start_urls = [
        'https://www.lesiteimmo.com/recherche?filter%5Btype%5D%5B0%5D=maison&filter%5Btype%5D%5B1%5D=appartement&filter%5Btransaction%5D=louer']
    country = 'france'
    locale = 'fr'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        try:
            for appartment in response.css("body > main > div:nth-child(3) > div > section > div.flex.flex-col-reverse > div:nth-child(2) > div:nth-child(2) > div.w-full.grid.xl\:grid-cols-2.gap-4>div"):
                yield Request(appartment.css("div.mt-2>a").attrib['href'],
                              callback=self.populate_item,
                              )
        except:
            pass

        try:
            next_page = response.xpath(
                "//a[contains(.,'Suivant')]").attrib['href']
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            'body > main > div:nth-child(3) > div > div.container.flex.flex-col-reverse.mx-auto.lg\:flex-col > div:nth-child(2) > div.px-4.mt-4 > div > div.px-2.lg\:flex-1 > div.flex.justify-between.-mx-2 > h1 > div.font-medium::text').get()
        rooms = title.split(" pièce")[0][-1]
        space = title.split(" m²")[0].split(" ")[-1]
        external_id = title.split(" (")[1].split(")")[0]

        address = title.split("m² ")[1].split(" (")[0]

        rent = response.css(
            'div.font-medium.text-gray-800::text').get().split("€")[0].strip()

        if " " in rent:
            rent_array = rent.split(" ")
            rent = rent_array[0] + rent_array[1]

        description = response.css(
            "p.mt-4.leading-relaxed.text-gray-800::text").get()

        images = response.css(
            'img.object-cover.w-full.h-full::attr(src)').extract()

        floor = None
        try:
            floor = response.css(
                "body > main > div:nth-child(3) > div > div.container.flex.flex-col-reverse.mx-auto.lg\:flex-col > div:nth-child(2) > div.px-4.mt-4 > div > div.px-2.lg\:flex-1 > div:nth-child(3) > dl > div:nth-child(5) > dd > ul > li::text").get().strip()
        except:
            pass

        room_features = response.css(
            "body > main > div:nth-child(3) > div > div.container.flex.flex-col-reverse.mx-auto.lg\:flex-col > div:nth-child(2) > div.px-4.mt-4 > div > div.px-2.lg\:flex-1 > div:nth-child(3) > dl > div:nth-child(2) > dd > ul>li")

        bathrooms = None
        try:
            for item in room_features:
                if "WC" in item.css('li::text').get():
                    bathrooms = item.css(
                        "li::text").get().strip()[0]
            int(bathrooms)
        except:
            pass

        monetary_features = response.css(
            "body > main > div:nth-child(3) > div > div.container.flex.flex-col-reverse.mx-auto.lg\:flex-col > div:nth-child(2) > div.px-4.mt-4 > div > div.px-2.lg\:flex-1 > div:nth-child(5) > div > ul>li")

        deposit = None
        for item in monetary_features:
            if "garantie " in item.css("li::text").get():
                deposit = item.css("li::text").get().strip().split(" ")[-2]

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", address)
        item_loader.add_value("floor", floor)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("rent", deposit)
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "0474600009")
        item_loader.add_value("landlord_email", "santino@homezon.it")
        item_loader.add_value("landlord_name", "Santino Balistreri ")

        yield item_loader.load_item()
