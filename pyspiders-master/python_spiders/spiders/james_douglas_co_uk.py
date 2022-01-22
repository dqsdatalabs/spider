# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..loaders import ListingLoader
from ..helper import extract_number_only,format_date,remove_white_spaces


class JamesDouglasCoUkSpider(scrapy.Spider):
    name = "james_douglas_co_uk"
    allowed_domains = ["james-douglas.co.uk"]
    start_urls = (
        'https://james-douglas.co.uk/properties-available/page/1/?department=residential-lettings',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath('.//li[contains(@class,"availability-to-let")]//a/@href').extract()
        listings = set(listings)
        for property_url in listings:
            yield scrapy.Request(url=property_url,
                                 callback=self.get_property_details,
                                 meta={'request_url': property_url}
                                 )

        next_page_url = response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_page_url:
            yield response.follow(
                url=next_page_url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')})
                        
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.xpath('//li[@class="reference-number"]/text()').extract_first()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value('external_id', external_id)

        item_loader.add_xpath('title', '//h1[@class="property_title entry-title"]/text()')
        dep = "".join(response.xpath("//li[@class='deposit']/text()").extract())
        if dep:
            item_loader.add_value('deposit', dep.strip())
        description = response.xpath('//div[@class="description-contents"]//text()').extract()
        if description:
            description = " ".join(description)
            item_loader.add_value('description', description)
            description = description.replace('-', " ")
            description = remove_white_spaces(description)
            epc_stringList = description.split("EPC")
            if len(epc_stringList) > 1:
                epc = epc_stringList[1].strip().split()[0]
                item_loader.add_value('energy_label', epc)
            deposit_stringList = description.split("Deposit")
            if len(deposit_stringList) > 1:
                deposit = deposit_stringList[-1].split()[0]
                if "£" in deposit:
                    item_loader.add_value('deposit', deposit)
                         

        property_type_text = response.xpath('.//li[@class="property-type"]/text()').extract_first()
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa']
        studio_types = ["studio"]
        property_type = None
        if property_type_text:
            property_type_text = property_type_text.strip().lower()
            if any([i in property_type_text for i in apartment_types]):
                property_type = "apartment"
                if "studio" in description.lower():
                    property_type = "studio"
            elif any([i in property_type_text for i in house_types]):
                property_type = "house"
            elif any([i in property_type_text for i in studio_types]):
                property_type = "studio"
        else:
            if "studio" in description.lower():
                property_type = "studio"
            elif any([i in description.lower() for i in apartment_types]):
                property_type = "apartment"
            elif any([i in description.lower() for i in house_types]):
                property_type = "house"
        if property_type:
            item_loader.add_value('property_type', property_type)
        else:
            return
 
        address = response.xpath('//h1[@class="property_title entry-title"]/text()').extract_first()
        if address:
            item_loader.add_value('address', address)
            city = address.split(", ")[-1]
            item_loader.add_value('city', city)

        room_count = response.xpath('//li[@class="bedrooms"]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', room_count)
        
        bathroom_count = response.xpath('//li[@class="bathrooms"]/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count)

        features = response.xpath('//div[@class="features"]//text()').extract()
        if features:
            featuresString = " ".join(features)

            # https://james-douglas.co.uk/property/roath-cardiff-2/
            if "parking" in featuresString.lower(): 
                item_loader.add_value('parking', True)

            if "elevator" in featuresString.lower() or 'lift' in featuresString.lower(): 
                item_loader.add_value('elevator', True)

            # https://james-douglas.co.uk/property/connaught-road-roath-cardiff-2/
            if "balcony" in featuresString.lower(): 
                item_loader.add_value('balcony', True)

            if "terrace" in featuresString.lower(): 
                item_loader.add_value('terrace', True)

            if "swimming pool" in featuresString.lower():
                item_loader.add_value('swimming_pool', True)

            if "washing machine" in featuresString.lower():
                item_loader.add_value('washing_machine', True)

            if "dishwasher" in featuresString.lower():
                item_loader.add_value('dishwasher', True)

            if "pets considered" in featuresString.lower(): 
                item_loader.add_value('pets_allowed', True)

        # https://james-douglas.co.uk/property/coburn-street-cathays-cardiff-2/
        furnished = response.xpath('//li[@class="furnished"]/text()').extract_first()
        if furnished:
            furnished = furnished.strip()
            if furnished.lower() == 'furnished':
                item_loader.add_value('furnished', True)
            # https://james-douglas.co.uk/property/cowbridge-rd-east-canton-cardiff/
            elif furnished.lower() == "unfurnished":
                item_loader.add_value('furnished', False)

        item_loader.add_xpath('rent_string', '//div[@class="price"]//text()')

        available_date_string = response.xpath('//li[@class="available-date"]/text()').extract_first()
        if available_date_string:
            available_date_string = available_date_string.strip()
            if available_date_string.lower() != "now":
                available_date_string = available_date_string.replace('st', '').replace('rd', '').replace('nd','').replace('th','')
                available_date = format_date(available_date_string, '%d %B %Y')
                item_loader.add_value('available_date', available_date)

        item_loader.add_xpath('images', '//div[@class="images"]//a/@href')
        item_loader.add_xpath('floor_plan_images', '//a[text()="Floorplan"]/@href')
        item_loader.add_value('landlord_name', "James Douglas Sales and Lettings")
        item_loader.add_value('landlord_phone', '02920 456 444')
        item_loader.add_value('landlord_email', 'info@james-douglas.co.uk')
        item_loader.add_value("external_source", "JamesDouglas_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value("external_link", response.meta.get("request_url"))

        self.position += 1
        item_loader.add_value("position", self.position)
        yield item_loader.load_item()
