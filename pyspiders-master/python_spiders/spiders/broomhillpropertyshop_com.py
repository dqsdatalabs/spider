# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
import re
import lxml 
import js2xml
import dateparser
from scrapy import Selector 
from scrapy import Request,FormRequest

class BroomhillPropertyShopCom(scrapy.Spider):
    name = "broomhillpropertyshop_com"
    # allowed_domains = ["broomhillpropertyshop.com"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="BroomHillPropertyShop_PySpider_united_kingdom_en"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.broomhillpropertyshop.com/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&bedrooms=&max_bedrooms=&showstc=on",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse)

    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for url in response.xpath('//div[@class="panel-body"]//a/@href').getall():
            url=f"https://www.broomhillpropertyshop.com/{url}"
            yield Request(url,callback = self.populate_item)
            seen = True
        if page == 2 or seen:
            p_url = f"https://www.broomhillpropertyshop.com/search/{page}.html?showstc=on&showsold=off&instruction_type=Letting&address_keyword=&bedrooms=&max_bedrooms="
            yield Request(
                p_url,
                dont_filter=True,
                callback=self.parse,
                meta={"page":page+1}
            )
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link",response.url)

        
        address = response.xpath('//p[@class="address"]/text()').extract_first()
        description = remove_unicode_char("".join(response.xpath('//div[@id="1"]/p//text()').extract()))
        features = " ".join(response.xpath('//div[@class="feature feature-orange"]/text()').extract())
        images = response.xpath('//img[@class="img-responsive"]/@src').extract()
        if images:
            images = [response.urljoin(img) for img in images]
        item_loader.add_value("external_source", self.external_source)
        # item_loader.add_value('property_type', response.meta.get('property_type'))

        item_loader.add_xpath('title', '//h1[@class="property-title"]/text()')
        item_loader.add_xpath('room_count', '//div[@class="col-xs-6 beds"]/text()')
        item_loader.add_xpath('bathroom_count', '//div[@class="col-xs-6 baths"]/text()')
        item_loader.add_value('address', address)
        if address:
            city = address.split(', ')[-1]
            item_loader.add_value('city', city)

        available_date = response.xpath('//p[@class="availability"]/text()').extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date)
            if date_parsed:
                item_loader.add_value('available_date', date_parsed.strftime("%Y-%m-%d"))
        
        rent = response.xpath('//p[@class="money"]/text()').extract()
        if rent:
            rent = remove_white_spaces(rent[0])
            if "pcm" in rent.lower():
                rent = extract_number_only(rent, thousand_separator=',', scale_separator='.')
                item_loader.add_value('rent_string', '£' + str(float(rent)))
            else:
                rent = extract_number_only(rent, thousand_separator=',', scale_separator='.')
                item_loader.add_value('rent_string', '£' + str(float(rent)*4))
        
        item_loader.add_value('images', images)
        item_loader.add_value('description', description)
        
        javascript = response.xpath('//script[contains(text(),"latlng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            item_loader.add_value('latitude', xml_selector.xpath('.//property[@name="lat"]/number/@value').extract_first())
            item_loader.add_value('longitude', xml_selector.xpath('.//property[@name="lng"]/number/@value').extract_first())
            
        item_loader.add_value('landlord_name', 'Broomhill Property Shop')
        item_loader.add_value('landlord_phone', '0114 266 6693')
        item_loader.add_value('landlord_email', 'admin@broomhillpropertyshop.co.uk')

        #https://www.broomhillpropertyshop.com/property/let/1378
        if "terrace" in features.lower() or "terrace" in description.lower():
            item_loader.add_value('terrace', True)
        #https://www.broomhillpropertyshop.com/property/let/1512
        if "balcony" in features.lower() or "balcony" in description.lower():
            item_loader.add_value('balcony', True)
        #https://www.broomhillpropertyshop.com/property/let/1383
        if "washing machine" in features.lower() or "washing machine" in description.lower():
            item_loader.add_value('washing_machine', True)
        #https://www.broomhillpropertyshop.com/property/let/1383
        if "dishwasher" in features.lower() or "dishwasher" in description.lower():
            item_loader.add_value('dishwasher', True)
        if "swimming pool" in features.lower() or "swimming pool" in description.lower():
            item_loader.add_value('swimming_pool', True)
        #https://www.broomhillpropertyshop.com/property/let/1383
        if "parking" in features.lower() or "parking" in description.lower():
            item_loader.add_value('parking', True)
        #https://www.broomhillpropertyshop.com/property/let/1526
        if "elevator" in features.lower() or "lift" in features.lower() or "elevator" in description.lower() or "lift" in description.lower():
            item_loader.add_value('elevator', True)
        #https://www.broomhillpropertyshop.com/property/let/1389
        if "furnished" in features.lower():
            if re.search(r"un[^\w]*furnished", features.lower()):
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        if "furnished" in description.lower():
            if re.search(r"un[^\w]*furnished", description.lower()):
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)


        yield item_loader.load_item()