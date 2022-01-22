# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = 'theonlinelettingagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    download_timeout=300
    external_source="Theonlinelettingagents_PySpider_united_kingdom_en"
    
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.theonlinelettingagents.co.uk/property-search/?wpp_search%5Bpagination%5D=on&wpp_search%5Bper_page%5D=10&wpp_search%5Bstrict_search%5D=false&wpp_search%5Bproperty_type%5D=commercial%2Cresidential&wpp_search%5Breference%5D=&wpp_search%5Bstatus%5D=To+Let&wpp_search%5Blandlords_property_type%5D=Flat%2FApartment&wpp_search%5Btown%5D=&wpp_search%5Bmanual_county%5D=-1&wpp_search%5Bpostcode%5D=&wpp_search%5Bbedrooms%5D=-1&wpp_search%5Bstudents_allowed%5D=-1&wpp_search%5Bpets_allowed%5D=-1",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.theonlinelettingagents.co.uk/property-search/?wpp_search%5Bpagination%5D=on&wpp_search%5Bper_page%5D=10&wpp_search%5Bstrict_search%5D=false&wpp_search%5Bproperty_type%5D=commercial%2Cresidential&wpp_search%5Breference%5D=&wpp_search%5Bstatus%5D=To+Let&wpp_search%5Blandlords_property_type%5D=Semi+Detached+House&wpp_search%5Btown%5D=&wpp_search%5Bmanual_county%5D=-1&wpp_search%5Bpostcode%5D=&wpp_search%5Bbedrooms%5D=-1&wpp_search%5Bstudents_allowed%5D=-1&wpp_search%5Bpets_allowed%5D=-1",
                "property_type" : "house"
            },
            {
                "url" : "https://www.theonlinelettingagents.co.uk/property-search/?wpp_search%5Bpagination%5D=on&wpp_search%5Bper_page%5D=10&wpp_search%5Bstrict_search%5D=false&wpp_search%5Bproperty_type%5D=commercial%2Cresidential&wpp_search%5Breference%5D=&wpp_search%5Bstatus%5D=To+Let&wpp_search%5Blandlords_property_type%5D=Detached+House&wpp_search%5Btown%5D=&wpp_search%5Bmanual_county%5D=-1&wpp_search%5Bpostcode%5D=&wpp_search%5Bbedrooms%5D=-1&wpp_search%5Bstudents_allowed%5D=-1&wpp_search%5Bpets_allowed%5D=-1",
                "property_type" : "house"
            },
            {
                "url" : "https://www.theonlinelettingagents.co.uk/property-search/?wpp_search%5Bpagination%5D=on&wpp_search%5Bper_page%5D=10&wpp_search%5Bstrict_search%5D=false&wpp_search%5Bproperty_type%5D=commercial%2Cresidential&wpp_search%5Breference%5D=&wpp_search%5Bstatus%5D=To+Let&wpp_search%5Blandlords_property_type%5D=Terraced+House&wpp_search%5Btown%5D=&wpp_search%5Bmanual_county%5D=-1&wpp_search%5Bpostcode%5D=&wpp_search%5Bbedrooms%5D=-1&wpp_search%5Bstudents_allowed%5D=-1&wpp_search%5Bpets_allowed%5D=-1",
                "property_type" : "house"
            },
            {
                "url" : "https://www.theonlinelettingagents.co.uk/property-search/?wpp_search%5Bpagination%5D=on&wpp_search%5Bper_page%5D=10&wpp_search%5Bstrict_search%5D=false&wpp_search%5Bproperty_type%5D=commercial%2Cresidential&wpp_search%5Breference%5D=&wpp_search%5Bstatus%5D=To+Let&wpp_search%5Blandlords_property_type%5D=Room+Only&wpp_search%5Btown%5D=&wpp_search%5Bmanual_county%5D=-1&wpp_search%5Bpostcode%5D=&wpp_search%5Bbedrooms%5D=-1&wpp_search%5Bstudents_allowed%5D=-1&wpp_search%5Bpets_allowed%5D=-1",
                "property_type" : "room"
            },
            {
                "url" : "https://www.theonlinelettingagents.co.uk/property-search/?wpp_search%5Bpagination%5D=on&wpp_search%5Bper_page%5D=10&wpp_search%5Bstrict_search%5D=false&wpp_search%5Bproperty_type%5D=commercial%2Cresidential&wpp_search%5Breference%5D=&wpp_search%5Bstatus%5D=To+Let&wpp_search%5Blandlords_property_type%5D=Bungalow&wpp_search%5Btown%5D=&wpp_search%5Bmanual_county%5D=-1&wpp_search%5Bpostcode%5D=&wpp_search%5Bbedrooms%5D=-1&wpp_search%5Bstudents_allowed%5D=-1&wpp_search%5Bpets_allowed%5D=-1",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')}) 

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[@class='property_title']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            ) 
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        address = response.xpath("//h1/../div/p/text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//dt[contains(.,'City')]/following-sibling::dd[1]/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        zipcode = response.xpath("//dt[contains(.,'Postcode')]/following-sibling::dd[1]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        if 'studio' in title.lower():
            item_loader.add_value("property_type", 'studio')
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        bathroom_count = response.xpath("//dt[contains(.,'Bathroom')]/following-sibling::dd[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        description = response.xpath("//div[@class='wpp_the_content']/p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if 'sq ft' in desc_html.lower() or 'sq. ft.' in desc_html.lower() or 'sqft' in desc_html.lower():
            square_meters = desc_html.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1]
            square_meters = str(int(float(square_meters.replace(',', '.').strip('+').strip('(')) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//dt[contains(.,'Bedroom')]/following-sibling::dd[1]/text()").get()
        if room_count:
            room_count = room_count.strip().replace('\xa0', '')
            if 'studio' in room_count.lower():
                room_count = '1'
            else:
                room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//dt[contains(.,'Rent')]/following-sibling::dd[1]/text()").get()
        if rent:
            if 'pw' in rent.lower() or 'per week':
                rent = rent.split('£')[-1].lower().split('p')[0].split('(')[0].strip().replace(',', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            elif 'pcm' in rent.lower():
                rent = rent.split('£')[-1].lower().split('p')[0].split('(')[0].strip().replace(',', '')
                item_loader.add_value("rent", str(int(float(rent))))
                item_loader.add_value("currency", 'GBP')

        external_id = response.xpath("//dt[contains(.,'Reference')]/following-sibling::dd[1]/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        available_date = response.xpath("//dt[contains(.,'Available From')]/following-sibling::dd[1]/text()").get()
        if available_date:
            if len(available_date.split('-')) > 2 or len(available_date.split('.')) > 2 or len(available_date.split('/')) > 2:
                if not '0/00/00' in available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[contains(@class,'gallery-thumbs')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//dt[contains(.,'Deposit')]/following-sibling::dd[1]/text()").get()
        if deposit:
            if not 'tbc' in deposit.lower():
                deposit = deposit.split('£')[-1].strip().replace(',', '')
                if deposit.replace('.', '').isnumeric():
                    item_loader.add_value("deposit", str(int(float(deposit))))

        parking = response.xpath("//li[contains(text(),'Garage') or contains(text(),'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        terrace = response.xpath("//dt[contains(.,'Detachment')]/following-sibling::dd[1]/text()[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        pets_allowed = response.xpath("//dt[contains(.,'Pets Allowed')]/following-sibling::dd[1]/text()").get()
        if pets_allowed:
            if pets_allowed.strip().lower() == 'yes':
                item_loader.add_value("pets_allowed", True)
            elif pets_allowed.strip().lower() == 'no':
                item_loader.add_value("pets_allowed", False)

        furnished = response.xpath("//dt[contains(.,'Furnishing')]/following-sibling::dd[1]/text()").get()
        if furnished:
            if furnished.strip().lower() == 'yes' or furnished.strip().lower() == 'optional':
                item_loader.add_value("furnished", True)
            elif furnished.strip().lower() == 'no':
                item_loader.add_value("furnished", False)

        item_loader.add_value("landlord_name", "The Online Letting Agents")
        item_loader.add_value("landlord_phone", "03300 883 973")
        item_loader.add_value("landlord_email", "info@theonlinelettingagents.co.uk")
     
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data