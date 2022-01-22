# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import format_date 
import dateparser
class KeyletSpider(scrapy.Spider):
    name = "keylet"
    allowed_domains = ["keylet.co.uk"]
    start_urls = (
        'http://www.keylet.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = 'https://keylet.co.uk/property/?wppf_search=to-rent&wppf_radius=3&wppf_orderby=nearest&wppf_view=list'
        yield scrapy.Request(url=start_urls, callback=self.parse, dont_filter=True)

    def parse(self, response, **kwargs):
        # parse the detail url
        links = response.xpath('//a[@rel="bookmark"]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
        if response.xpath('//div[contains(@class, "wppf_result_container")]/following-sibling::nav//a[contains(@class, "next")]/@href'):
            next_link = response.xpath('//div[contains(@class, "wppf_result_container")]/following-sibling::nav//a[contains(@class, "next")]/@href').extract_first()
            
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True)
    
    def get_property_details(self, response):
        #parse detail information
        property_type_site = ''.join(response.xpath('//span[@class="wppf_subhead"]/text()').extract())
        if 'house' in property_type_site.lower():
            property_type = 'house'
        elif 'apartment' in property_type_site.lower():
            property_type = 'apartment'
        elif 'flat' in property_type_site.lower():
            property_type = 'apartment' 
        else:
            property_type = ''
        if property_type: 
            external_link = response.url
            external_id = response.xpath('//strong[contains(text(), "Ref")]/following-sibling::text()').extract_first('').strip()
            address = response.xpath('//strong[contains(text(), "Location")]/following-sibling::text()').extract_first('').strip()
            zipcode = address.split(', ')[-1]
            city = address.split(', ')[-2]
            available_date_text = response.xpath('//strong[contains(text(), "Available")]/following-sibling::text()').extract_first('')
            available_date = format_date(available_date_text, '%d %B %Y')
            room_count = response.xpath('//strong[contains(text(), "Bedroom")]/following-sibling::text()').extract_first('').strip()
            bathrooms = response.xpath('//div[strong[.="Bathrooms:"]]/text()').extract_first('').strip() 
            rent_string = ''.join(response.xpath('//h2[@class="up-price"]/text()').extract())
            try:
                lat_lon_text = re.search(r'new google\.maps\.LatLng\((.*?)\)',response.text).group(1)
                lat = lat_lon_text.split(',')[0]
                lng = lat_lon_text.split(',')[1]  
            except:
                lat = lng = '' 
            available_date_text = response.xpath('//strong[contains(text(), "Available")]/following-sibling::text()').extract_first('').split(' ')[-1]
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('external_id', str(external_id))
            item_loader.add_xpath('title', '//title/text()')
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_xpath('description', '//div[contains(@class, "wppf_property_about")]//p//text()')
            if rent_string:
                if "pw" in rent_string:
                    rent = rent_string.lower().split('£')[-1].split('pw')[0].strip().replace(',', '').replace('\xa0', '')
                    item_loader.add_value("rent", str(int(float(rent)*4))) 
                    item_loader.add_value("currency", 'GBP')
                else:
                    item_loader.add_value('rent_string', rent_string)
            item_loader.add_xpath('images', '//div[@id="wppf_slideshow"]//img/@src')
            if lat: 
                item_loader.add_value('latitude', str(lat))
            if lng: 
                item_loader.add_value('longitude', str(lng))
            item_loader.add_value('room_count', str(room_count))
            if available_date_text:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))            
            item_loader.add_value('bathroom_count', str(bathrooms))
            
            furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]//text()").get()
            if furnished:
                if "unfurnished" in furnished.lower():
                    item_loader.add_value("furnished",False)
                elif "furnished" in furnished.lower():
                    item_loader.add_value("furnished",True)
            parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
            if parking:
                if "no " in parking.lower():
                    item_loader.add_value("parking",False)
                else:
                    item_loader.add_value("parking",True)
            pets_allowed = response.xpath("//li[contains(.,'Pets ')]//text()").get()
            if pets_allowed:
                if " no " in pets_allowed.lower():
                    item_loader.add_value("pets_allowed",False)
                else:
                    item_loader.add_value("pets_allowed",True)
            balcony = response.xpath("//li[contains(.,'Balcony')]//text()").get()
            if balcony:
                item_loader.add_value("balcony",True)
            energy = response.xpath("//h3[.='Energy Performance Certificates']/following-sibling::div[1]/img/@src").get()
            if energy:
                energy_label = energy.split('_')[-1].split(".")[0].strip()
                if energy_label.isdigit():
                    item_loader.add_value("energy_label", energy_label_calculate(energy_label))
            item_loader.add_value('landlord_name', 'Keylet Sales & Lettings')
            item_loader.add_value('landlord_email', 'executive@keylet.co.uk')
            item_loader.add_value('landlord_phone', '02920489000')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 20:
        energy_label = "G"
    elif energy_number > 20 and energy_number <= 38:
        energy_label = "F"
    elif energy_number > 38 and energy_number <= 54:
        energy_label = "E"
    elif energy_number > 54 and energy_number <= 68:
        energy_label = "D"
    elif energy_number > 68 and energy_number <= 80:
        energy_label = "C"
    elif energy_number > 80 and energy_number <= 91:
        energy_label = "B"
    elif energy_number > 91:
        energy_label = "A"
    return energy_label
