# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
import re
from scrapy import Selector 
from datetime import datetime

class JBrownCom(scrapy.Spider):
    name = "jbrown_com"
    allowed_domains = ["jbrown.com"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = ["https://www.jbrown.com/property-search~for=2"]
        for url in start_urls:
            yield scrapy.Request(url = url,
                                callback = self.parse,
                                meta = {'request_url': url})

    def parse(self, response, **kwargs):
        
        listings = response.xpath('//a[contains(text(),"View Details")]/@href').extract()
        for url in listings:
            yield scrapy.Request(
                url = response.urljoin(url),
                callback = self.get_property_details,
                meta = {'request_url' : response.urljoin(url)})
        
        if len(listings) == 10:
            next_page_url = response.xpath('//a[contains(text(),"Next")]/@href').extract_first()
            if next_page_url:
                yield scrapy.Request(
                        url=response.urljoin(next_page_url),
                        callback=self.parse,
                        meta={'request_url':response.urljoin(next_page_url)})

    def get_property_details(self, response):

        external_link = response.meta.get('request_url')
        description = remove_unicode_char("".join(response.xpath('//div[@class="details-description"]//p/text()').extract()))
        address = response.xpath('//h4[@id="myModalLabel"]/text()').extract_first()
        features = response.xpath('//div[@id="features"]/text()').extract_first()

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "JBrown_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split('pid=')[-1])
        item_loader.add_xpath('title','//head/title/text()')
        if address:
            address = address.replace("Book a viewing for ", "")
            item_loader.add_value('address', address)
            city = address.split(',')[0]
            zipcode = address.split(',')[-1]
            item_loader.add_value('city', remove_white_spaces(city))
            item_loader.add_value('zipcode', remove_white_spaces(zipcode))

        rent = response.xpath("//h3[@class='price-details']/strong[1]/text()").get()
        if rent:
            price = rent.split(" ")[0].replace(",","").replace("£","")
            item_loader.add_value('rent', int(float(price)))
        item_loader.add_value("currency", "GBP")
        
        item_loader.add_xpath('images', '//div[@class="fotorama details-fotorama"]//img[@title=""]/@src')
        item_loader.add_value('description', description)
        
        property_type = "".join(response.xpath('//ul[@class="spec-list"]//i[@class="fa fa-home"]/../text()').extract())
        if get_p_type_string(property_type):
            item_loader.add_value('property_type', get_p_type_string(property_type))
        else: return
        room_count = response.xpath('//ul[@class="spec-list"]//i[@class="fa fa-bed"]/../text()').extract()
        if room_count:
            item_loader.add_value('room_count', extract_number_only(room_count[-1]))

        bathroom_count = "".join(response.xpath('//ul[@class="spec-list"]//i[@class="fa fa-bath"]/../text()').extract())
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            if bathroom_count.isdigit():
                item_loader.add_value('bathroom_count', bathroom_count)
            
        #https://www.jbrown.com/property-search~action=detail,pid=3869
        pets = response.xpath('//ul[@class="spec-list"]//i[@class="fa fa-paw"]/../text()').extract_first()
        if pets:
            pets = pets[-1]
            if "yes" in pets.lower():
                item_loader.add_value('pets_allowed', True)
            else:
                item_loader.add_value('pets_allowed', False)
        
        square_meters = "".join(response.xpath('//li[contains(.,"sqft")]/text()').extract())
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0]
            if square_meters !="0":
                sqm = str(int(int(square_meters)* 0.09290304))
                item_loader.add_value('square_meters',sqm )

        if "available now" in description.lower():
            available_date = str(datetime.now().day) + '-' + str(datetime.now().month) + '-' + str(datetime.now().year)
            item_loader.add_value('available_date', format_date(available_date,date_format="%d/%m/%Y"))

        #https://www.jbrown.com/property-search~action=detail,pid=4431
        parking = "".join(response.xpath('//ul[@class="spec-list"]//i[@class="fa fa-car"]/../text()').extract())
        if parking:
            if "yes" in parking.lower():
                item_loader.add_value('parking', True)
            else:
                item_loader.add_value('parking', False)
                
        elif "parking" in features.lower() or "parking" in description.lower():
            item_loader.add_value('parking', True)
        #https://www.jbrown.com/property-search~action=detail,pid=3869
        if "terrace" in features.lower() or "terrace" in description.lower():
            item_loader.add_value('terrace', True)
        #https://www.jbrown.com/property-search~action=detail,pid=3869
        if "balcony" in features.lower() or "balcony" in description.lower():
            item_loader.add_value('balcony', True)
        if "washing machine" in features.lower() or "washing machine" in description.lower():
            item_loader.add_value('washing_machine', True)
        if "dishwasher" in features.lower() or "dishwasher" in description.lower():
            item_loader.add_value('dishwasher', True)
        #https://www.jbrown.com/property-search~action=detail,pid=3736
        if "swimming pool" in features.lower() or "swimming pool" in description.lower():
            item_loader.add_value('swimming_pool', True)
        
        #https://www.jbrown.com/property-search~action=detail,pid=3817
        if "elevator" in features.lower() or "lift" in features.lower() or "elevator" in description.lower() or "lift" in description.lower():
            item_loader.add_value('elevator', True)
        #https://www.jbrown.com/property-search~action=detail,pid=3869
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

        item_loader.add_xpath('landlord_name','//h6[@class="details-subtitle marg-t-20"]/text()')
        landlord_phone = response.xpath('//i[@class="fa fa-phone"]/../text()').extract()
        if landlord_phone:
            item_loader.add_value('landlord_phone', landlord_phone[-1])
        landlord_email = response.xpath('//i[@class="fa fa-envelope"]/../text()').extract()
        if landlord_email:
            item_loader.add_value('landlord_email', landlord_email[1])

        self.position += 1
        item_loader.add_value('position', self.position)

        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "end of terrace" in p_type_string.lower()):
        return "house"
    else:
        return None