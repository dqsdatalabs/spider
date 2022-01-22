# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek 

import js2xml
import lxml
import scrapy
from scrapy import Selector
 
from ..helper import convert_string_to_numeric
from ..loaders import ListingLoader


class FortesshomesComSpider(scrapy.Spider):
    name = 'fortesshomes_com'
    allowed_domains = ['fortesshomes.com']
    start_urls = ['https://www.fortesshomes.com/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.fortesshomes.com/search/?showstc%2Cshowsold=on&instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Apartment',
                'property_type': 'apartment',
                'param': 'Apartment'},
            {
                'url': 'https://www.fortesshomes.com/search/?showstc%2Cshowsold=on&instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Flat',
                'property_type': 'apartment',
                'param': 'Flat'},
            {
                'url': 'https://www.fortesshomes.com/search/?showstc%2Cshowsold=on&instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Studio',
                'property_type': 'studio',
                'param': 'Studio'}, ]

        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type'),
                                       'param': url.get('param')})

    def parse(self, response, **kwargs):
        page = response.meta.get("page", 2)
        seen=False
        listings = response.xpath('.//div[contains(@class,"property-image")]/a/@href').extract()
        if listings:
            for property_item in listings:
                yield scrapy.Request(
                    url=f"https://www.fortesshomes.com{property_item}",
                    callback=self.get_property_details,
                    meta={'request_url': f"https://www.fortesshomes.com{property_item}",
                          'property_type': response.meta.get('property_type')})
                seen = True

        if page==2 or seen:
            yield scrapy.Request(
                url=response.urljoin(
                    f"https://www.fortesshomes.com/search/{page}.html?showstc%2Cshowsold=on&instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type={response.meta.get('param')}"),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'), "page":page+1, "param":response.meta.get('param')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.xpath('.//span[@itemprop="price"]/strong/text()').extract_first()
        floor_plan = [f"https://www.fortesshomes.com{plan}" for plan in response.xpath('.//div[@id="property-floorplans"]/img/@src').extract()]
        
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('request_url').split("property-details/")[-1].split("/")[0])
        item_loader.add_xpath('title', './/title/text()')
        item_loader.add_xpath('address', './/span[@itemprop="name"]/text()')
        item_loader.add_xpath('description', './/p[@class="short-desc"]/text()')
        item_loader.add_xpath('images', './/div[@id="property-carousel"]//div[@class="item"]//img/@src')
        item_loader.add_value('landlord_name', 'Fortess Homes')
        item_loader.add_value('landlord_email', 'info@fortesshomes.com')
        item_loader.add_value('landlord_phone', '0207 482 1150')
        item_loader.add_value('floor_plan_images', floor_plan)

        javascript = response.xpath('.//script[contains(text(),"renderStreetview")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            lat_lng_string = xml_selector.xpath('.//identifier[@name="show"]/../../arguments/string/text()').extract_first()
            lat_lng = lat_lng_string.split("q=")[-1].split("%2C")
            item_loader.add_value('latitude', lat_lng[0])
            item_loader.add_value('longitude', lat_lng[1])

        room_count = response.xpath('.//img[contains(@src,"bed")]/following::text()').extract_first()
        if room_count:
            item_loader.add_value('room_count',room_count)
        # elif response.meta["property_type"] == "studio":
        #     item_loader.add_value('room_count', "1")
        item_loader.add_xpath('bathroom_count', './/img[contains(@src,"bath")]/following::text()')

        energy_label = response.xpath('.//img[@alt="epc icon"]/parent::*/@href').extract_first()
        if energy_label:
            energy_label = energy_label.split('_')[-1].split('.')[0]
            energy_label = convert_string_to_numeric(energy_label, FortesshomesComSpider)
            if 92 <= energy_label <= 100:
                item_loader.add_value('energy_label', 'A')
            if 81 <= energy_label <= 91:
                item_loader.add_value('energy_label', 'B')
            if 69 <= energy_label <= 80:
                item_loader.add_value('energy_label', 'C')
            if 55 <= energy_label <= 68:
                item_loader.add_value('energy_label', 'D')
            if 39 <= energy_label <= 54:
                item_loader.add_value('energy_label', 'E')
            if 21 <= energy_label <= 38:
                item_loader.add_value('energy_label', 'F')
            if 1 <= energy_label <= 20:
                item_loader.add_value('energy_label', 'G')

        if item_loader.get_output_value('address'):
            city_zip = item_loader.get_output_value('address').split(',')
            item_loader.add_value('city', city_zip[-1].strip())
            # zip not present ...lat and lng are mentioned for reference

        if "PCM" in rent:
            item_loader.add_value('rent_string', f"£{rent}")
        else:
            pcm_rent = convert_string_to_numeric(rent, FortesshomesComSpider) * 4
            item_loader.add_value('rent_string', f"£{pcm_rent}")

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Fortesshomes_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
