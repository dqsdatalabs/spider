# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import  extract_rent_currency, remove_white_spaces, format_date
import re
from datetime import datetime

class BrinkrileySpider(scrapy.Spider):
    
    name = 'brinkriley_co_uk'
    allowed_domains = ['brinkriley.co.uk']
    start_urls = ['https://www.brinkriley.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator=','
    scale_separator='.'
    position = 0
        
    def start_requests(self):
        start_urls = ['https://brinkriley.co.uk/property/page/1/?department=residential-lettings']
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url':url})
            
    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//*[@class="thumbnail"]//a/@href').extract():
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url})
        
        if len(response.xpath('.//*[@class="thumbnail"]//a')) > 0:
            current_page = re.findall(r"(?<=page/)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page/)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url})
        
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        title = response.xpath('.//*[contains(@class,"property_title")]/text()').extract_first()

        property_type = response.xpath('.//*[@class="property-type"]/text()').get()
        property_type = get_p_type_string(property_type)
        if property_type: item_loader.add_value("property_type", property_type)
        else: return

        city = response.xpath("//h1/text()").get()
        if city: item_loader.add_value("city", city.split(",")[-1].strip())
      
        furnished = response.xpath('.//*[@class="furnished"]/text()').extract_first()
        bathroom_count = response.xpath('.//*[@class="bathrooms"]/text()').extract_first() 
        availability = response.xpath('.//*[@class="availability"]/text()').extract_first()
        if availability and "under offer" in availability.lower():
            return 

        room_count = response.xpath('.//*[@class="bedrooms"]/text()').extract_first()
        dates = response.xpath('.//*[@class="available-date"]/text()').extract_first()
        rent_string =response.xpath('.//*[@class="price"]/text()').extract_first()
        if not re.search(r'let.*[^\w]*agreed',str(availability),re.IGNORECASE):
            item_loader.add_value('external_link', response.meta.get('request_url'))
            item_loader.add_xpath('external_id','.//*[@class="reference-number"]/text()')
            item_loader.add_xpath('title','.//*[contains(@class,"property_title")]/text()')
            item_loader.add_xpath('description','.//*[@class="description-contents"]//text()')
            item_loader.add_xpath('images','.//a[@class="propertyhive-main-image"]/@href')
            item_loader.add_xpath('floor_plan_images','.//*[contains(text(),"Floorplan")]/@href')
            item_loader.add_xpath('address','.//*[contains(@class,"property_title")]/text()')

            description = item_loader.get_output_value('description')
            
            #https://brinkriley.co.uk/property/the-old-post-office-bishop-street-leicester-le1-3/
            #room_count
            if room_count == None or room_count == 0:
                if property_type and property_type == "studio":
                    item_loader.add_value('room_count','1')
                elif property_type and property_type == "apartment":
                    item_loader.add_value('room_count','1')
            elif room_count:
                item_loader.add_value('room_count',room_count)
            if bathroom_count:
                item_loader.add_value('bathroom_count',bathroom_count)
            if furnished and remove_white_spaces(furnished) == 'Furnished':
                item_loader.add_value('furnished',True)
            if dates:
                r = re.compile('(\w+)\s*(\d+).*(\d{4})')
                if r.search(dates):
                    date_str = r.search(dates).group(1).lower()[:3]+' '+r.search(dates).group(2)+' '+r.search(dates).group(3)
                    available_date =datetime.strptime(date_str, '%b %d %Y').strftime('%d/%m/%Y')
                    item_loader.add_value('available_date', format_date(available_date, "%d/%m/%Y"))
            if rent_string and any(word in rent_string.lower() for word in ['week','pw','pppw','pcw']):
                rent = rent_string.split("£")[1].split(" ")[0].strip()
                item_loader.add_value('rent_string','£'+str(int(rent)*4))
            elif rent_string and any(word in rent_string.lower() for word in ['month','pm','pcm']):
                item_loader.add_value('rent_string',rent_string)
            else:
                item_loader.add_value('rent_string',rent_string)
            address = item_loader.get_output_value('address')
            post = remove_white_spaces(address.split(',')[-1])
            zipcode = re.search(r'(([A-Z][A-HJ-Y]?\d[A-Z\d]?|ASCN|STHL|TDCU|BBND|[BFS]IQQ|PCRN|TKCA) ?\d[A-Z]{2}|BFPO ?\d{1,4}|(KY\d|MSR|VG|AI)[ -]?\d{4}|[A-Z]{2} ?\d{2}|GE ?CX|GIR ?0A{2}|SAN ?TA1)$',address,re.IGNORECASE)
            if zipcode:
                item_loader.add_value('zipcode',zipcode.group())
            elif zipcode == None and ''.join(post.split()).isalpha()==False:
                item_loader.add_value('zipcode',post)
            item_loader.add_value("external_source", "Brinkriley_PySpider_{}_{}".format(self.country, self.locale))
            item_loader.add_value('landlord_phone','0800 228 9437')
            item_loader.add_value('landlord_email','info@brinkriley.co.uk')
            item_loader.add_value('landlord_name','brink riley')
            self.position += 1
            item_loader.add_value('position', self.position)
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "penthouse" in p_type_string.lower() or "duplex" in p_type_string.lower() or "triplex" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("chalet" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maison" in p_type_string.lower() or "house" in p_type_string.lower() or "home" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None