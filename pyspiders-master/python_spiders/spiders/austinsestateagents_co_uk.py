# -*- coding: utf-8 -*-
# Author: Gabriel Francis
# Team: Sabertooth
import scrapy
from ..loaders import ListingLoader
from ..helper import format_date
import re

class AustinsestateagentsCoUkSpider(scrapy.Spider):
    name = "austinsestateagents_co_uk"
    allowed_domains = ["www.austinsestateagents.co.uk"]
    start_url = 'https://www.austinsestateagents.co.uk/property-search/?acs-action=advanced-search'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position = 0

    def start_requests(self):

        formdata = {'acs[form]': '1424',
            'acs[wp][taxonomy_propertycategories][value]': '18'}

        yield scrapy.FormRequest(
            url=self.start_url,
            formdata = formdata, 
            callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath('.//li[contains(@class,"let")]')
        for listing in listings:
            check = listing.xpath('.//span[@class="badge"]/text()').extract_first()
            if check in ['Let ','Under Offer ']:
                continue
            property_url = listing.xpath('.//a/@href').extract_first()
            rent_string = listing.xpath('.//div[@class="property-details-container"]/span[@class="float-right"]/text()').extract_first()
            yield scrapy.Request(
                url=property_url, 
                callback=self.get_property_details, 
                meta={'request_url':property_url,
                    'rent_string':rent_string})

    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_value("rent_string", response.meta.get('rent_string'))

        item_loader.add_xpath('title','.//h1/text()')
        item_loader.add_xpath('address','.//h1/text()')

        room_count = max(int(response.xpath('.//label[contains(text(),"Bedrooms")]/following-sibling::span/text()').extract_first()),1)
        item_loader.add_xpath('room_count',str(room_count))
        item_loader.add_xpath('bathroom_count','.//label[contains(text(),"Bathrooms")]/following-sibling::span/text()')

        item_loader.add_xpath('description','.//div[@id="about"]/p/text()')
        item_loader.add_xpath('images','.//img[contains(@class,"property-image")]/@src')

        overview = ' '.join(response.xpath('.//div[@class="overview-text"]/p/text()').extract())

        if any(item in item_loader.get_output_value('description').lower()+overview.lower() \
            for item in ['studio','bedsit']):
            item_loader.add_value('property_type', 'studio')
        elif any(item in item_loader.get_output_value('description').lower()+overview.lower() \
            for item in ['apartment','single bedroom']):
            item_loader.add_value('property_type', 'apartment')
        elif any(item in item_loader.get_output_value('description').lower()+overview.lower() \
            for item in ['house']):
            item_loader.add_value('property_type', 'house')
        elif any(item in item_loader.get_output_value('description').lower()+overview.lower() \
            for item in ['student accommodation']):
            item_loader.add_value('property_type', 'student_apartment')
        elif any(item in item_loader.get_output_value('description').lower()+overview.lower() \
            for item in ['room']):
            item_loader.add_value('property_type', 'room')
        else:
            return

        latlng = response.xpath('.//iframe[contains(@src,"google.com/maps")]/@src').extract_first()
        if latlng:
            latlng = re.findall(r'(?<=q=\()(\d+\.\d+,[-|]\d+\.\d+)(?=\))',latlng)
            if latlng:
                latitude,longitude = latlng[0].split(',')[0], latlng[0].split(',')[1]
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)

        furnished = response.xpath('.//label[contains(text(),"Furnished")]/following-sibling::text()').extract_first()
        if 'Unfurnished' in furnished:
            item_loader.add_value('furnished',False)
        elif 'Furnished' in furnished:
            item_loader.add_value('furnished',True)

        parking = ' '.join(response.xpath('.//label[contains(text(),"Off Street Parking") or contains(text(),"Garage")]/following-sibling::text()').extract())
        if 'Yes' in parking.lower():
            item_loader.add_value('parking',True)
        else:
            item_loader.add_value('parking',False)

        pets = response.xpath('.//label[contains(text(),"Pets")]/following-sibling::text()').extract_first()
        if 'Yes' in pets.lower():
            item_loader.add_value('pets_allowed',True)
        else:
            item_loader.add_value('pets_allowed',False)

        available_date = re.findall(r'(?<=Available )\d{1,2}(?:th|rd|st|nd) \w+ \d{4}',item_loader.get_output_value('description'))
        if available_date:
            available_date = available_date[0].replace('th','').replace('rd','').replace('st','').replace('nt','')
            item_loader.add_value('available_date',format_date(available_date,date_format='%d %B %Y'))

        deposit = re.findall(r'(?<=Deposit )([£]\d+\.\d+)',item_loader.get_output_value('description'))
        if deposit:
            item_loader.add_value('deposit',deposit[0])

        item_loader.add_value('landlord_name', 'Austins Estate Agents')
        item_loader.add_value('landlord_email', 'info@austinsestateagents.co.uk')
        item_loader.add_value('landlord_phone', '01902 244200')

        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.split('_')[0].capitalize(), self.country, self.locale))
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
