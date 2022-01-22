from re import escape
import re
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import math


class RentalpropertymanagersCaSpider(scrapy.Spider):
    name = 'rentalpropertymanagers_ca'
    allowed_domains = ['rentalpropertymanagers.ca']
    start_urls = [
        'http://www.rentalpropertymanagers.ca/rental-listings-in-greater-vancouver/?wplpage=1']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#wpl_property_listing_container > div.wpl_property_listing_list_view_container > div.wpl-row.wpl-expanded.wpl-small-up-1.wpl-medium-up-2.wpl-large-up-3.wpl_property_listing_listings_container.clearfix > div"):
            url = appartment.css("a.view_detail").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

        try:
            next_page = response.css('li.next > a').attrib['href']
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            'div.wpl_prp_container_content > div.wpl-row.wpl-expanded.wpl_prp_container_content_title > div.wpl-large-10.wpl-medium-10.wpl-small-12.wpl-columns > h1::text').get()

        address = response.css(
            'div.wpl_prp_container_content > div.wpl-row.wpl-expanded.wpl_prp_container_content_title > div.wpl-large-10.wpl-medium-10.wpl-small-12.wpl-columns > h2 > span.wpl-location::text').get().split('-')[-1]
        city = address.split(', ')[-1]

        zipcode = None
        try:
            zipcode = response.css('#wpl-dbst-show43 > span::text').get()
        except:
            pass

        description = ''
        description_array = response.css(
            "div.wpl_prp_container_content > div:nth-child(3) > div.wpl-large-8.wpl-medium-7.wpl-small-12.wpl_prp_container_content_left.wpl-column > div.wpl_prp_show_detail_boxes.wpl_category_description > div.wpl_prp_show_detail_boxes_cont > div > div > p::text").extract()

        for item in description_array:
            description += item

        space = response.css('span.wpl-built-up-area::text').get()
        if ',' in space:
            space = space.split(',')
            space = space[0]+space[1]
        space = int(int(space.split(' ')[0].strip()) * 0.0929)

        try:
            rent = response.css(
                '#wpl_prp_container93 > div.wpl_prp_container_content > div:nth-child(3) > div.wpl-large-4.wpl-medium-5.wpl-small-12.wpl_prp_container_content_right.wpl-column > div.wpl_prp_right_boxes.details > div.wpl_prp_right_boxes_content > div.wpl_prp_right_boxe_details_bot > div::text').get().split('$')[1].split(' ')[0]

            if ',' in rent:
                rent = rent.split(',')
                rent = rent[0]+rent[1]
            rent = int(rent)
        except:
            rent = None

        lat = None
        lng = None
        try:
            coords = response.xpath('/html/body/script[20]/text()').get()
            lat = coords.split("ws_lat = '")[1].split("';")[0]
            lng = coords.split("ws_lon = '")[1].split("';")[0]
        except:
            pass

        feats1 = response.css(
            'div.wpl-small-up-1.wpl-medium-up-1.wpl-large-up-2.wpl_prp_show_detail_boxes_cont > div')

        bathrooms = None
        bedrooms = None
        parking = None
        parking = None
        avaialble_date = None
        furnished = None
        property_type = None
        for item in feats1:
            if "Bathrooms" in item.css('::text').get():
                bathrooms = item.css('span::text').get()
                bathrooms = math.ceil(float(bathrooms))
            elif "Bedrooms" in item.css('::text').get():
                bedrooms = item.css('span::text').get()
            elif "Parking Space" in item.css('::text').get():
                parking = item.css('span::text').get()
                if int(parking) > 0:
                    parking = True
                else:
                    parking = False
            elif "Availability" in item.css('::text').get():
                avaialble_date = item.css('span::text').get()
                if "RENTED" in avaialble_date:
                    return
            elif "Furnished" in item.css('::text').get():
                furnished = item.css('span::text').get()
                if "unfurnished" in furnished.lower():
                    furnished = False
                else:
                    furnished = True
            elif "Property Type" in item.css('::text').get():
                property_type = item.css('span::text').get()
                if "Condo" in property_type:
                    property_type = 'apartment'
                elif "house" in property_type:
                    property_type = 'house'

        feats2 = response.css(
            'div.wpl-small-up-1.wpl-medium-up-1.wpl-large-up-2.wpl_prp_show_detail_boxes_cont > div')

        feats3 = response.css(
            'div.wpl-small-up-1.wpl-medium-up-1.wpl-large-up-2.wpl_prp_show_detail_boxes_cont')

        feats2 = feats2+feats3

        dishwasher = None
        washing_machine = None
        pool = None
        for item in feats2:
            if "Dishwasher" in item.css('::text').get():
                dishwasher = True
            elif "Washing Machine" in item.css('::text').get():
                washing_machine = True
            elif "Swimming Pool" in item.css('::text').get():
                pool = True

        feats4 = response.css(
            'div.wpl-small-up-1.wpl-medium-up-1.wpl-large-up-2.wpl_prp_show_detail_boxes_cont > div')

        pets = None
        for item in feats4:
            if "Pet" in item.css('::text').get():
                pets = item.css('span::text').get()
                if "Not" in pets:
                    pets = False
                else:
                    pets = True

        images = response.xpath('//a/img/@src').extract()

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", int(int(int(space))*10.764))
        item_loader.add_value("room_count", bedrooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("available_date", avaialble_date)
        item_loader.add_value("parking", parking)
        item_loader.add_value("swimming_pool", pool)
        item_loader.add_value("pets_allowed", pets)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_phone", "778-862-8321")
        item_loader.add_value("landlord_email", "lotusyuen@gmail.com")
        item_loader.add_value(
            "landlord_name", "Lotus Yuen PREC, REMAX CREST Realty")

        yield item_loader.load_item()
