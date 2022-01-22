# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..helper import extract_number_only
from ..loaders import ListingLoader
import re

class OctopuspropertyCoUkSpider(scrapy.Spider):
    name = "octopusproperty_co_uk"
    allowed_domains = ["octopusproperty.co.uk"]
    start_urls = ['http://octopusproperty.co.uk/Rent-Property']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="property-list-list"]')
        for listing in listings:
            property_url = listing.xpath('.//a[contains(@id,"cphContent_cphMain_rptListView_hlProperty")]/@href').extract_first()
            city = listing.xpath('.//span[contains(@id,"cphContent_cphMain_rptListView_spanCity")]/text()').extract_first()
            rent_string = listing.xpath('.//span[contains(@id,"cphContent_cphMain_rptListView_lblPrice")]/text()').extract_first()
            bathroom_count = listing.xpath('.//li[contains(text(),"Bathrooms")]/following-sibling::li/span/text()').extract_first()
            room_count = listing.xpath('.//li[contains(text(),"Beds")]/following-sibling::li/span/text()').extract_first()
            parking = listing.xpath('.//li[contains(text(),"Garages")]/following-sibling::li/span/text()').extract_first()
            yield scrapy.Request(
                url=property_url, 
                callback=self.get_property_details, 
                meta={'request_url':property_url,
                    'rent_string':rent_string,
                    'bathroom_count':bathroom_count,
                    'room_count':room_count,
                    'parking':parking,
                    'city':city})

    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        external_id = re.findall(r'(?<=Property/)(\d{5})',response.meta.get('request_url'))
        if external_id:
            item_loader.add_value("external_id", external_id[0])
        item_loader.add_value("room_count", extract_number_only( response.meta.get('room_count')))
        item_loader.add_value("city", response.meta.get('city'))
        if response.meta.get('bathroom_count')!='0':
            item_loader.add_value("bathroom_count", response.meta.get('bathroom_count'))
        if response.meta.get('parking'):
            if 'no' in response.meta.get('parking').lower():
                item_loader.add_value("parking", False)
            elif 'yes' in response.meta.get('parking').lower():
                item_loader.add_value("parking", True)

        if 'monthly' in response.meta.get('rent_string').lower():
            item_loader.add_value("rent_string", response.meta.get('rent_string'))
        elif 'pppw' in response.meta.get('rent_string').lower():
            rent = str(float(extract_number_only(response.meta.get('rent_string'),scale_separator='.',thousand_separator=','))*4)
            currency = response.meta.get('rent_string')[0]
            item_loader.add_value("rent_string", currency+rent)

        item_loader.add_xpath('title','.//h4[@id="cphContent_cphMain_h2PropTitle"]/text()')
        item_loader.add_xpath('address','.//h4[@id="cphContent_cphMain_h2PropTitle"]/text()')

        item_loader.add_value('zipcode',item_loader.get_output_value('address').split(', ')[-1])

        item_loader.add_xpath('description','.//p[@id="cphContent_cphMain_pDescription"]/following-sibling::p//text()')
        item_loader.add_xpath('images','.//img[contains(@id,"cphContent_cphMain_rptMedia_imgMedia")]/@src')

        furnished = response.xpath('.//div[contains(text(),"Furnishing")]/following-sibling::div/span/text()').extract_first()
        if furnished:
            if 'fully furnished' in furnished.lower():
                item_loader.add_value("furnished", True)

        property_type = response.xpath('.//div[contains(text(),"Type")]/following-sibling::div/span/text()').extract_first()
        if property_type:

            if "terrace" in property_type.lower():
                item_loader.add_value("terrace", True)

            if property_type.lower() in ['apartment','flat','flat share','terraced house']:
                item_loader.add_value("property_type", 'apartment')
            elif property_type.lower() in ['maisonette','house','bungalow','house share','end terrace house','semi-detached house','detached house']:
                item_loader.add_value("property_type", 'house')
            elif property_type.lower() in ['studio']:
                item_loader.add_value("property_type", 'studio')
            else:
                return

        energy_label = re.findall(r'(?<=EPC score: )(\w)',item_loader.get_output_value('description'))
        if energy_label:
            item_loader.add_value('energy_label',energy_label[0])

        item_loader.add_xpath('landlord_phone','.//span[@id="cphContent_cphMain_lblAdminTel"]/text()')
        item_loader.add_xpath('landlord_email','.//span[@id="cphContent_cphMain_hlAdminEmail"]/text()')
        item_loader.add_value('landlord_name','Octopus Property')

        if not item_loader.get_collected_values("landlord_email"):
            item_loader.add_value("landlord_email", "info@octopusproperty.co.uk")

        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.split('_')[0].capitalize(), self.country, self.locale))
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
