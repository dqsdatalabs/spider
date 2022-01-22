# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_rent_currency, extract_number_only, remove_white_spaces
import re
import lxml,js2xml
from parsel import Selector

class CumbrianpropertiesSpider(scrapy.Spider):
    name = "cumbrian-properties_co_uk"
    allowed_domains = ["cumbrian-properties.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    listing_agreed=[]
    position=0

    def start_requests(self):
        url = 'https://cumbrian-properties.co.uk/lettings'
        property_types = [{
            'name':'house',
            'type':'house'},
            {'name':'flat',
            'type':'apartment'},
            {'name':'bungalow',
            'type':'house'},
            {'name':'lodge',
            'type':'lodge'}]
        for property_type in property_types:
            yield scrapy.FormRequest(url=url,
                                 callback=self.parse,
                                 formdata = {'propertyType': property_type.get('name')},
                                 meta={'request_url':url,'property_type':property_type.get('type')})
            
    def parse(self, response, **kwargs):
        listing = response.xpath('.//*[contains(@href,"let-property-details-page")]/@href').extract()
        agreed = response.xpath('.//div[@class="propertyImage"]')
        for i in agreed:
            z = i.xpath('.//p/text()').extract_first()
            self.listing_agreed.append(z)
        new_list = zip(self.listing_agreed,listing)
        for i,property_url in new_list:
            if i==None:
                yield scrapy.Request(
                    url=property_url,
                    callback=self.get_property_details,
                    meta={'request_url': property_url}
                    )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.meta.get('request_url')
        item_loader.add_value('external_link', external_link)
        external_id = re.search(r'(?<=id=)\d+',external_link)
        item_loader.add_value('external_id',external_id.group())
        item_loader.add_xpath('images','.//*[@class="group"]/@href')
        title = response.xpath('.//*[@property="og:title"]/@content').extract_first()
        item_loader.add_value('title',title.split(' - ')[0])

        description = " ".join(response.xpath("//h3[contains(@id,'overview')]//following-sibling::p//text()").getall())
        if description: 
            item_loader.add_value("description", description.strip())
        
        key_info = response.xpath('.//div[@class="col large-9"]/ul/li/text()').extract()

        f_text = "".join(response.xpath("//div[@id='content']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else: return

        EPC = response.xpath('.//img[@alt="EPC Certificate"]/@src').extract_first()
        if EPC:
            ratingValue=EPC.split('_')[-2]
            if ratingValue.isnumeric():
                ratingValue=int(ratingValue)
                if ratingValue>=92 and ratingValue<=100:
                    epc_class='A'
                elif ratingValue>=81 and ratingValue<=91:
                    epc_class='B'
                elif ratingValue>=69 and ratingValue<=80:
                    epc_class='C'
                elif ratingValue>=55 and ratingValue<=68:
                    epc_class='D'
                elif ratingValue>=39 and ratingValue<=54:
                    epc_class='E'
                elif ratingValue>=21 and ratingValue<=38:
                    epc_class='F'
                elif ratingValue>=1 and ratingValue<=20:
                    epc_class='G'
                item_loader.add_value('energy_label',epc_class)
        features = ', '.join(key_info).lower()
        room_count = re.search(r'\d+\s*(?=bedroom)',features)
        if room_count and extract_number_only(room_count.group()) != '0':
            item_loader.add_value('room_count',extract_number_only(room_count.group()))
        elif room_count == None or extract_number_only(room_count.group()) == '0' and item_loader.get_output_value('property_type') == 'studio':
            item_loader.add_value('room_count','1')
        bathroom_count = re.search(r'\d+\s*(?=bathroom)',features)
        if bathroom_count and extract_number_only(bathroom_count.group()) != '0' :
            item_loader.add_value('bathroom_count',extract_number_only(bathroom_count.group()))
        # https://cumbrian-properties.co.uk/let-property-details-page/?id=30243668
        if 'unfurnished' in features:
            item_loader.add_value('furnished',False)
        elif 'furnished' in features and 'part furnished' not in features:
            item_loader.add_value('furnished',True)
        if 'terrace' in features:
            item_loader.add_value('terrace',True)
        if 'balcony' in features:
            item_loader.add_value('balcony',True)
        if 'swimming pool' in features or 'pool' in features:
            item_loader.add_value('swimming_pool',True)
        # https://cumbrian-properties.co.uk/let-property-details-page/?id=30223648
        if 'parking' in features:
            item_loader.add_value('parking',True)
        if 'dishwasher' in features:
            item_loader.add_value('dishwasher',True)
        if 'lift' in features or 'elevator' in features:
            item_loader.add_value('elevator',True)
        # https://cumbrian-properties.co.uk/let-property-details-page/?id=30243668
        if 'no pets allowed' in features or 'no pets' in features:
            item_loader.add_value('pets_allowed',False)
        elif 'pets allowed' in features:
            item_loader.add_value('pets_allowed',True)
        
        address = response.xpath("//title//text()").get()
        if address:
            address = address.split("-")[0].strip()
            if "," in address and "CA11" not in address:
                city = address.split(",")[-1].strip()
            elif "CA11" not in address:
                city = address.split(" ")[-1]
            else:
                city = address.split(",")[-2]
            item_loader.add_value("city", city)
            item_loader.add_value("address", address)

        rent = response.xpath("//h3[contains(.,'£')]//text()").get()
        if rent:
            rent = rent.split("£")[1].split("/")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")


        deposit = "".join(response.xpath("//div[@class='section-content relative']/div/div/p[contains(.,'DEPOSIT')]/text()").getall())
        if deposit:
            dep = deposit.split("DEPOSIT")[1].strip()
            item_loader.add_value("deposit", dep)

        javascript = response.xpath('.//script[contains(text(),"lng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property[@name="lat"]/number/@value').extract_first()
            longitude = selector.xpath('.//property[@name="lng"]/number/@value').extract_first()
            if latitude and longitude and latitude !='0' and longitude != '0':
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)
        item_loader.add_value("external_source", "Cumbrian_properties_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('landlord_name','Cumbrian Properties')  
        item_loader.add_xpath('landlord_phone','.//*[@class="fas fa-phone"]/following-sibling::text()')
        item_loader.add_xpath('landlord_email','.//*[@class="teamBlock"]//*[contains(@href,"mailto")]/text()')
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "unit" in p_type_string.lower() or "home" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None
                    