# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..helper import convert_string_to_numeric, convert_to_numeric
from ..loaders import ListingLoader


class StelfortCoUkSpider(scrapy.Spider):
    name = 'stelfort_co_uk'
    allowed_domains = ['stelfort.co.uk']
    start_urls = ['https://stelfort.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            'https://www.stelfort.co.uk/properties/?page=1&propind=L&country=&town=&area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&PropType=&Furn=&Avail=&orderBy=PriceSearchAmount&orderDirection=DESC&areaId=&lat=&lng=&zoom=&searchbymap=&maplocations=&hideProps=1&location=&businessCategoryId=1&searchType=grid&sortBy=highestPrice',
        ]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse, )

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="photo-cropped"]/a/@href').extract()
        for property_item in listings:
            yield scrapy.Request(
                url=f"https://www.stelfort.co.uk/{property_item}",
                callback=self.get_property_details,
                meta={'request_url': f"https://www.stelfort.co.uk/{property_item}"}
            )

        next_page_url = response.xpath('.//a[contains(text(),"next")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(f"https://www.stelfort.co.uk/{next_page_url}"),
                callback=self.parse,
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath('.//span[@class="bedsWithTypePropType"]/text()').extract_first()
        apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico", "penthouse", "duplex"]
        house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa']
        studio_types = ["studio"]
        if property_type.lower() in apartment_types:
            property_type = "apartment"
        if property_type.lower() in house_types:
            property_type = "house"
        if property_type.lower() in studio_types:
            property_type = "studio"
        else:
            property_type = "apartment"

        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', response.meta.get('request_url'))

        ref = response.xpath('.//div[@class="reference"]/text()').extract_first().split(":")[-1].strip()
        item_loader.add_value('external_id', ref)
        
        title = "".join(response.xpath('.//div[@class="bedswithtype"]//text()').extract())
        item_loader.add_value('title', title)

        rent = convert_string_to_numeric(response.xpath('.//span[@class="displayprice"]/text()').extract_first(), StelfortCoUkSpider) * 4
        item_loader.add_value('rent_string', f"£{rent}")

        item_loader.add_xpath('description', './/div[@class="description"]/text()')
        item_loader.add_xpath('bathroom_count', './/span[@class="bathrooms"]/text()')
        item_loader.add_xpath('images', './/img[@class="propertyimage"]//@src')

        map_cords_url = response.xpath('.//div[@id="maplinkwrap"]/a/@href').extract_first()
        if map_cords_url:
            map_cords = map_cords_url.split("lat=")[-1].split("&lng=")
            item_loader.add_value('latitude', map_cords[0])
            item_loader.add_value('longitude', map_cords[1].split('&')[0])

        address = response.xpath('.//div[@class="address"]//text()').extract_first()
        item_loader.add_value('address', address)
        if item_loader.get_output_value('address'):
            city_zip = item_loader.get_output_value('address').split(',')
            city = address.split(",")[-2].strip()
            item_loader.add_value('city', city)
            # city cannot be extracted due to variations in address format
            # item_loader.add_value('city', city_zip[-2].strip())
            if any(ch.isdigit() for ch in city_zip[-1].split(' ')[-1]):
                item_loader.add_value('zipcode', city_zip[-1].split(' ')[-1])

        floorplan = response.xpath('.//a[@class="floorplantoggle"]/@href').extract_first()
        if floorplan and floorplan != "#":
            item_loader.add_xpath('floor_plan_images', floorplan)

        bed = convert_to_numeric(response.xpath('.//span[@class="beds"]/text()').extract_first())
        if bed:
            item_loader.add_xpath('room_count', './/span[@class="beds"]/text()')
        elif item_loader.get_output_value('property_type') == 'studio':
            item_loader.add_value('room_count', 1)

        features = ' '.join(response.xpath('.//div[@class="twocolfeaturelist"]//li/text()').extract())

        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)
            # https://www.stelfort.co.uk/property/1777/?propInd=L&page=2&pageSize=12&orderBy=PriceSearchAmount&orderDirection=DESC&businessCategoryId=1&searchType=grid&hideProps=1&stateValues=1
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)
        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)
        if "parking" in features.lower():
            item_loader.add_value('parking', True)
            # https://www.stelfort.co.uk/property/1777/?propInd=L&page=2&pageSize=12&orderBy=PriceSearchAmount&orderDirection=DESC&businessCategoryId=1&searchType=grid&hideProps=1&stateValues=1
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        if "furnished" in features.lower():
            if "unfurnished" in features.lower() and ' furnished' not in features.lower():
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        item_loader.add_value('landlord_name', 'Stelfort')
        item_loader.add_value('landlord_email', 'info@stelfort.co.uk')
        item_loader.add_value('landlord_phone', '020 7263 3555')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Stelfort_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
