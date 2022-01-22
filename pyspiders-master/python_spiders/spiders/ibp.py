# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
import json
from python_spiders.helper import string_found, remove_white_spaces

def extract_city_zipcode(_city_zip):
    zipcode, city =  _city_zip.split(" ")
    return zipcode, city

class IbpSpider(scrapy.Spider):
    name = 'ibp'
    allowed_domains = ['ibp'] 
    start_urls = ['https://www.ibp.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.ibp.be/en-GB/List/21', 'property_type': 'apartment'},
            {'url': 'https://www.ibp.be/en-GB/List/21', 'property_type': 'house'}
        ]
        for url in start_urls:
            if 'apartment' in url.get('url'): 
                data = {
                    '__RequestVerificationToken': 'HUruwWJUkrBBnt3QFMVhLGdIsb5uV5QtJp53aPmQk3P_UHw-5VTzMeCPSGYyhUo5vR8eJYDq7pH4QpKhLC3M0V3fADmOlqNVDkRO422sUmw1',
                    'ListID': '21',
                    'SearchType': 'ToRent',
                    'EstateRef': '',
                    'SelectedType': '2',
                    'InvestmentEstate': 'false',
                    'MinPrice': '',
                    'MaxPrice': '',
                    'Rooms': '0',
                    'SortParameter': '1',
                    'Furnished': 'false',
                    'GroundMinArea': ''
                }
            else:
                data = {
                    '__RequestVerificationToken': 's9AbZBOil7oL8jeD0ocS-UeMJaGgxDqBt4BQrL6DaOv4nXJlSC6hMpcxPx4e7Iv0qpvot1zIUoeIJIn85NWYskjt2AEf_zDfu-t5b_1G0yw1',
                    'ListID': '21',
                    'SearchType': 'ToRent',
                    'EstateRef': '',
                    'SelectedType': '1',
                    'InvestmentEstate': 'false',
                    'MinPrice': '',
                    'MaxPrice': '',
                    'Rooms': '0',
                    'SortParameter': '1',
                    'Furnished': 'false',
                    'GroundMinArea': ''
                }
            yield scrapy.Request(
                url=url.get('url'),
                body=json.dumps(data),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//a[@class="estate-thumb"]')
        for link in links:
            status = link.xpath(".//span[contains(@class,'banner')]/span/text()").get()
            if status and ("rented" in status.lower() or "loué" in status.lower()):
                continue
            if link.xpath('./@href').extract_first():
                url = response.urljoin(link.xpath('./@href').extract_first())
                yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//a[contains(text(), ">")]/@href'):
            next_link = response.urljoin(response.xpath('//a[contains(text(), ">")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
             
    def get_property_details(self, response):

        prop_control = response.xpath("//h1/text()").get()
        if prop_control and ("office" in prop_control.lower() or "workshop" in prop_control.lower() or "miscellaneous" in prop_control.lower() or "mixt building" in prop_control.lower()): return

        external_id = response.xpath('//th[contains(text(), "Reference")]/following-sibling::td/text()').extract_first('').strip()
        external_link = response.url
        room_count_text = str(response.xpath('//th[contains(text(), "bedrooms")]/following-sibling::td/text()').extract_first('').strip())
        try:
            if int(room_count_text) > 0:
                room_count = room_count_text
            else:
                room_count = ''
        except:
            room_count = ''
        square_meters = str(response.xpath('//th[contains(text(), "Habitable")]/following-sibling::td/text()').extract_first('').strip())
        
        
        address = ' '.join([x.strip() for x in response.xpath('//i[contains(@class, "fa-map-marker")]/..//text()').extract()])
        city_zip = ''.join([y.strip() for y in response.xpath('//i[@class="fa"]/following-sibling::text()').extract()]) 
        city = extract_city_zipcode(city_zip.strip())
        zipcode = response.xpath("//h1//text()").get()
        if zipcode:
            zipcode = zipcode.split("rent -")[1].strip().split(" ")[0]
        parking_text = response.xpath('//th[contains(text(), "Garage")]/following-sibling::td/text()').extract_first('').strip()
        terrasse_text = response.xpath('//th[contains(text(), "Terrace")]/following-sibling::td/text()').extract_first('').strip()
        elevator_text = response.xpath('//th[contains(text(), "Elevator")]/following-sibling::td/text()').extract_first('').strip() 
        furnished_text = response.xpath('//th[contains(text(), "Furnished")]/following-sibling::td/text()').extract_first('').strip()  
        rent = response.xpath('//div[@class="estate-feature"]//span[@class="estate-text-emphasis"]/text()').extract_first('').strip()
        if rent:
            rent = rent.split("€")[0].strip().replace(",","")
        available_date = response.xpath("//th[contains(.,'Details_Availability')]//following-sibling::td//text()").get()
        if available_date and "/" in available_date:
            available_date = available_date.split("/")[-1]
            if not int(available_date) > 2019:
                return
        item_loader = ListingLoader(response=response)
        property_type = item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_link', external_link)
        item_loader.add_xpath('title', '//meta[@property="og:title"]/@content')
        item_loader.add_value('address', address)
        item_loader.add_xpath('description', '//h2[contains(text(), "Description")]/following-sibling::p//text()')
        item_loader.add_value('rent', rent)
        item_loader.add_value("currency", "EUR")
        item_loader.add_xpath('images', '//ul[contains(@class, "slider-main-estate")]/li/a/@href')
        item_loader.add_value('square_meters', square_meters)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        if string_found(['yes'], parking_text):
            item_loader.add_value('parking', True)
        if string_found(['yes'], elevator_text):
            item_loader.add_value('elevator', True)
        if string_found(['yes'], terrasse_text):
            item_loader.add_value('terrace', True)
        if string_found(['yes'], furnished_text):
            item_loader.add_value('furnished', True)
        
        import dateparser
        available_date = response.xpath("//th[contains(.,'Availab')]/following-sibling::td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        bathroom_count = response.xpath("//th[contains(.,'bathroom')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
            
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('landlord_name', 'IBP')
        item_loader.add_value('landlord_email', 'info@ibp.be')
        item_loader.add_value('landlord_phone', '+32 2 743 03 80')
        item_loader.add_value('external_source', 'Ibp_PySpider_belgium_nl')
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower() or "house" in p_type_string.lower() or "room" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None


         