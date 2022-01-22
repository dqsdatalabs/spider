# -*- coding: utf-8 -*-

import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class CityroomsComSpider(scrapy.Spider):
    name = 'thecityrooms_com'
    execution_type = 'development'
    country = 'united_kingdom'
    locale = 'en'
    allowed_domains = ['cityrooms.com']
    start_urls = [
        'https://www.cityrooms.com/results?displayOrder=tenadate&propSearchType=R&radius=1000&market=1']
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    def parse(self, response):
        for appartment in response.css('div.results-list'):
            url = "https://www.cityrooms.com/" + \
                  appartment.css(
                      'div.results-image.photoLabel>a').attrib['href']
            yield Request(url, callback=self.populate_item, dont_filter=True)

        try:
            next_page = response.css(
                'ul.pagination>li:last-child>a').attrib['href']
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        about_the_room = response.css(
            'div.details-information>div.detail-value::text')
        images = response.css(
            'img.sp-image.img-responsive::attr(data-src)').extract()
        title = response.css(
            'div.detail-address>h1::text').get()

        rent = response.css(
            '#viewing-contain > div:nth-child(1) > p > span::text').get()
        if "," in rent:
            rent_array = rent.split("£")[1].split(",")
            rent = rent_array[0] + rent_array[1]
        else:
            rent = rent.split("£")[1]

        if '.' in rent:
            rent = rent.split('.')[0]

        deposit = response.css(
            '#viewing-contain > div:nth-child(2) > p > span::text').get()

        if "," in deposit:
            deposite_array = deposit.split("£")[1].split(",")
            deposit = deposite_array[0] + deposite_array[1]
        else:
            deposit = deposit.split("£")[1]

        if '.' in deposit:
            deposit = deposit.split('.')[0]

        square_meters = response.css(
            '#detail-room-blurb > div > div:nth-child(7)::text').get()
        sq_m = 0.00

        if "SqM" in square_meters:
            sq_m = str(square_meters.split(" ")[0])

        item_loader.add_value("external_link", response.url)
        # item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", about_the_room.getall()[-1].strip())
        item_loader.add_value("title", title)
        item_loader.add_value("description", array_filter(
            response.css('div.details-information>p::text').getall()))

        item_loader.add_value("address", title.split(" in ")[1])
        item_loader.add_value("property_type", "room")
        item_loader.add_value("square_meters", sq_m)
        item_loader.add_value("room_count", "1")

        bathroom_count = response.css(
            'div.detail-attribute.numberofbathrooms>div.detail-value::text').get()
        if bathroom_count:
            bathroom_count = bathroom_count
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("available_date", about_the_room.getall()[0])

        item_loader.add_value("furnished", True if response.css(
            'div.detail-feature.furnished') else None)
        item_loader.add_value("washing_machine", True if response.css(
            'div.detail-feature.washingmachine') else None)

        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("deposit", deposit)

        item_loader.add_value("landlord_phone", '44 20 7790 5577')
        item_loader.add_value("landlord_email", "info@cityrooms.com")
        item_loader.add_value("landlord_name", "cityrooms")

        yield item_loader.load_item()


def array_filter(array):
    description = ''
    for i in array:
        description += i
    return description
