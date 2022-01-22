# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from time import strptime
import scrapy
from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, format_date, extract_number_only
# from ..user_agents import random_user_agent
# from geopy.geocoders import Nominatim
import re
import lxml.etree
import js2xml
from scrapy import Selector
from datetime import datetime
import dateparser

class AlexCrownSpider(scrapy.Spider):

    name = "AlexCrown_PySpider_unitedkingdom_en"
    allowed_domains = ['alexcrown.co.uk']
    start_urls = ['http://alexcrown.co.uk/']
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_url = ["http://alexcrown.co.uk/let/property-to-let//page/1"]
        for url in start_url:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url,
                                 'page':1})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="module-content"]')
        for property_item in listings:
            url = "http://alexcrown.co.uk" + property_item.xpath('.//a/@href').extract_first()
            yield scrapy.Request(
                url = url,
                callback=self.get_property_details,
                meta={'request_url' : url,
                'room_count': property_item.xpath('.//div[@class="featured-stats"]/text()').extract()[0],
                'bathroom_count':property_item.xpath('.//div[@class="featured-stats"]/text()').extract()[1]})
        
        if len(listings) == 12:    
            current_page = re.findall(r"(?<=page/)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page/)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(url= next_page_url,
            callback=self.parse,
            meta={'page': response.meta.get('page')+1,
            'request_url' : next_page_url})

                    
    def get_property_details(self, response):
        
        no_page = response.xpath("//div[@id='resultsheader']/h1/text()").extract_first()
        if no_page:
            if "sorry" in no_page.lower():
                return
        dontallow=response.xpath("//ul[@class='featureslist']//li//span//text()").getall()
        if dontallow:
            for i in dontallow:
                if "office space" in i.lower():
                    return 
        external_link = response.meta.get('request_url')
        address = response.xpath('//h1[@class="details_h1"]/text()').extract_first()
    
        item_loader = ListingLoader(response=response)

        

        stuido = response.xpath("//div[@class='details-stats']/span//text()[contains(.,'Studio')]").extract_first()
        if stuido:
            item_loader.add_value('room_count', "1")
        else:
            room_count = response.xpath("//div[@class='details-stats']/span//text()").extract_first()
            if room_count:
                item_loader.add_value('room_count', room_count)

        bathroom_count = response.xpath("//div[@class='details-stats']/span[2]/text()").get()
        if bathroom_count: item_loader.add_value('bathroom_count', bathroom_count.strip())
   
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_source', "AlexCrown_PySpider_united_kingdom_en")
        item_loader.add_value('external_id',external_link.split('/')[4])
        item_loader.add_xpath('title',"//title/text()")     
        item_loader.add_value('address',address)
        item_loader.add_value('city',address.split(', ')[1])
        item_loader.add_value('zipcode',address.split(', ')[-1].replace('\r\n','').replace(' ',''))
        # item_loader.add_value('rent_string',currency + rent)
        # item_loader.add_xpath('property_type','//div[@class="details-stats"]/span/text/text()')
        item_loader.add_xpath('description','//div[@id="module-description"]/div/text()')
        item_loader.add_value('landlord_name','Alex Crown')
        item_loader.add_xpath('landlord_phone','//a[contains(@href,"tel:")]/text()')
        item_loader.add_xpath('landlord_email','//a[contains(@href,"mailto")]/text()')

        
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'slickGalery')]/div//a/@href").extract()]
        if images:
            item_loader.add_value("images", images)

        apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico",
                           "penthouse", "duplex", "dakappartement", "triplex"]
        house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'huis', 'cottage', 'student property']
        studio_types = ["studio"]

        features = "".join(response.xpath('//div[@id="module-features"]//span/text()').extract())
        description = "".join(response.xpath('//div[@id="module-description"]/div/text()').extract())
        
        for i in studio_types:
            if i in description.lower():
                item_loader.add_value('property_type','studio')
        for i in house_types:
            if i in description.lower():
                item_loader.add_value('property_type','house')
        for i in apartment_types:
            if i in description.lower():
                item_loader.add_value('property_type','apartment')
    
        prop_types = response.xpath("//div[@class='details-stats']/span/text/text()").get()
        if prop_types and "studio" in prop_types.lower(): item_loader.add_value('property_type', 'studio')
        
        # features = "".join(response.xpath('//div[@id="module-features"]//span/text()').extract())
        # description = "".join(response.xpath('//div[@id="module-description"]/div/text()').extract())
        property1=item_loader.get_output_value("external_link")
        if property1 and "flat" in property1:
            item_loader.add_value("property_type","apartment")

        propertypecheck=item_loader.get_output_value("property_type")
        if not propertypecheck:
            property=response.xpath("//ul[@class='featureslist']//li//span//text()").getall()
            if property:
                for i in property:
                    if "flat" in i.lower():
                        item_loader.add_value("property_type","apartment")
                        
            

        if "parking" in features.lower() or "parking" in description.lower():
            item_loader.add_value('parking',True)
        
        if "terrace" in features.lower() or "terrace" in description.lower():
            item_loader.add_value('terrace',True)

        if "swimming pool" in features.lower() or "swimming pool" in description.lower():
            item_loader.add_value('swimming_pool',True)
        
        if "elevator" in features.lower() or "elevator" in description.lower():
            item_loader.add_value('elevator',True)
        
        if "balcony" in features.lower() or "balcony" in description.lower():
            item_loader.add_value('balcony',True)

        if "unfurnished" in features.lower() or "unfurnished" in description.lower():
            item_loader.add_value('furnished',False)
        elif " furnished " in features.lower() or " furnished " in description.lower():
            item_loader.add_value('furnished',True)

        available_date=response.xpath("//ul/li[span[contains(.,'Available')]]/span/text()").get()
        if available_date:
            date2 =  available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        javascript = response.xpath('.//*[contains(text(),"initMap")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property[@name="lat"]/string/text()').extract_first()
            longitude = selector.xpath('.//property[@name="lng"]/string/text()').extract_first()
            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)
            
        rent = response.xpath("//h1//text()[contains(.,'per month')]").get()
        if rent:
            item_loader.add_value("rent", rent.split("per month")[0].split("£")[-1].strip())
            item_loader.add_value("currency", "EUR")
        else:
            currency = response.xpath('//span[@class="nativecurrencysymbol"]/text()').extract_first()
            rent = response.xpath("//h1[@class='details_h1']/span/span[@class='nativecurrencyvalue']/text()").extract_first()
            if rent:
                price = rent.replace(",","").strip().split(".")[0].strip()
                item_loader.add_value('rent',int(float(price))*4)
                item_loader.add_value('currency',"GBP")

        return item_loader.load_item()