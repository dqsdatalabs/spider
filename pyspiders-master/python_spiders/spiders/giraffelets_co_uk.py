# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import  remove_white_spaces

class GiraffeletsCoUkSpider(scrapy.Spider):
    name = 'giraffelets_co_uk'
    allowed_domains = ['giraffelets.co.uk']
    start_urls = ['https://www.giraffelets.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            'https://www.giraffelets.co.uk/properties/?page=1&propind=L&country=&town=&area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&PropType=&Furn=&Avail=&O=PriceSearchAmount&Dir=ASC&areaId=&lat=&lng=&zoom=&searchbymap=&maplocations=&hideProps=1&location=&searchType=list']
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[contains(@class,"searchprop")]')
        if listings:
            for property_item in listings:
                property_url = property_item.xpath('.//div[contains(@class,"photo")]//a/@href').extract_first()
                bed = property_item.xpath('.//span[@class="beds"]/text()').extract_first()
                bath = property_item.xpath('.//span[@class="bathrooms"]/text()').extract_first()
                yield scrapy.Request(
                    url=f"https://www.giraffelets.co.uk/{property_url}",
                    callback=self.get_property_details,
                    meta={'request_url': f"https://www.giraffelets.co.uk/{property_url}",
                          "bed": bed,
                          "bathroom": bath}
                )
        next_page_url = response.xpath('.//a[contains(text(),"next")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url, }
            )

    def get_property_details(self, response):
        map_cords_url = response.xpath('.//div[@id="maplinkwrap"]/a/@href').extract_first()
        ref_id = response.xpath('.//div[@class="reference"]/text()').extract_first().split(": ")[-1]
        item_loader = ListingLoader(response=response)
        property_type = response.xpath('.//span[@class="bedsWithTypePropType"]/text()').extract_first()
        if property_type:
            if get_p_type_string(property_type): 
                item_loader.add_value("property_type", get_p_type_string(property_type))
            else: 
                property_type = "".join(response.xpath('.//div[@class="description"]/text()').extract())
                if get_p_type_string(property_type): 
                    item_loader.add_value("property_type", get_p_type_string(property_type))
       
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', ref_id)
        item_loader.add_xpath('rent_string', './/span[@class="displayprice"]/text()')
        item_loader.add_xpath('title', './/title/text()')
        item_loader.add_xpath('address', './/div[@class="propertydet"]//div[@class="address"]/text()')
        item_loader.add_xpath('description', './/div[@class="description"]/text()')
        item_loader.add_xpath('images', './/img[@class="propertyimage"]/@src')
        item_loader.add_value('landlord_name', 'Giraffelets')
        item_loader.add_value('landlord_email', 'info@giraffelets.co.uk')
        item_loader.add_value('landlord_phone', '0191 276 0550')

        item_loader.add_value('room_count', response.meta.get('bed'))
        item_loader.add_value('bathroom_count', response.meta.get('bathroom'))
        item_loader.add_xpath('floor_plan_images', './/div[@id="hiddenfloorplan"]//img/@src')

        if map_cords_url:
            map_cords = map_cords_url.split("lat=")[-1].split('&lng=-')
            item_loader.add_value('latitude', map_cords[0])
            item_loader.add_value('longitude', map_cords[-1].split("&")[0])
            
        address = item_loader.get_output_value('address')
        if address:
            city = re.sub(('-|–|—'),',',address).split(',')[-1]
            item_loader.add_value('city', remove_white_spaces(city))

        self.position += 1
        item_loader.add_value('position', self.position)

        item_loader.add_value("external_source","{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None