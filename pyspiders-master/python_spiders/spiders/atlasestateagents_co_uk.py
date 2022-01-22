# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import re
import js2xml
import lxml.etree
import scrapy
from scrapy import Selector
from ..helper import extract_number_only, extract_rent_currency, remove_white_spaces,format_date
from ..loaders import ListingLoader
from math import ceil


class AtlasestateagentsCoUkSpider(scrapy.Spider):
    name = 'atlasestateagents_co_uk'
    allowed_domains = ['www.atlasestateagents.co.uk']
    start_urls = ['https://www.atlasestateagents.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    external_source='Atlasestateagents_PySpider_united_kingdom_en'
    locale = 'en'
    thousand_separator=','
    scale_separator='.'
    position = 0

    def start_requests(self):     
        start_urls = ["https://www.atlasestateagents.co.uk/property-search?sort=3&availability=2&location=Liverpool%2C+United+Kingdom&radius=50&min_price=&max_price=&min_beds=&max_beds=&type=&added="]   
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                )

    def parse(self, response, **kwargs):
        listings = response.xpath('//div[contains(@class,"property-search-details")]')
        
        for property_item in listings:
            external_link = property_item.xpath('.//a/@href').extract_first()
            external_id = re.findall(r'property-\d+', external_link)[0]
            rent_string = remove_white_spaces(property_item.xpath(".//span[contains(@class,'display_price')]/text()").extract_first())

            yield scrapy.Request(
                url = external_link,
                callback=self.get_property_details,
                meta={'external_link' : external_link,
                    'external_id' : external_id,
                    'rent_string' : rent_string,
                    })
                    

        next_page = response.xpath('//a[contains(@aria-label,"Next")]/@href').extract_first()   
        if next_page:
            next_page_url = 'https://www.atlasestateagents.co.uk/property-search' + next_page
            yield response.follow(
                url=next_page_url,
                callback=self.parse,
                )

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['mobile home','park home','character property','chalet', 'bungalow', 'maison', 'house', 'home', ' villa ', 'holiday complex', 'cottage', 'semi-detached']
        studio_types = ["studio"]
        property_type = " ".join(response.xpath("//h5[contains(.,'Key Features')]//following-sibling::ul//text()").getall())
        if not "warehouse" in property_type.lower():
            if any (i in  property_type for i in studio_types):
                item_loader.add_value('property_type','studio')
            elif any (i in property_type for i in apartment_types):
                item_loader.add_value('property_type','apartment')
            elif any (i in property_type for i in house_types):
                item_loader.add_value('property_type','house')
            else:
                return
        else:
            return

        item_loader.add_value('external_link', response.meta.get('external_link'))
        item_loader.add_value('external_id',response.meta.get('external_id'))
        item_loader.add_xpath('title', '//div[@class="display_type"]/h2/text()')
        item_loader.add_xpath('images','.//div[@class="carousel-inner"]//@href')        
        rent = response.xpath("//div[contains(@class,'price')]//text()").get()
        if rent:
            if "week" in rent.lower():
                rent = rent.split("£")[1].split(" ")[0].split('\xa0per')[0].strip()
                rent = int(rent)*4
            else:
                rent = rent.split("£")[1].split(" ")[0].split('\xa0per')[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        desc = " ".join(response.xpath("//div[contains(@id,'details')]/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        item_loader.add_xpath('address','//div[@class="display_address"]/h1/text()')
        item_loader.add_value('zipcode', item_loader.get_output_value('address').split(', ')[-1])
        item_loader.add_value('city', 'Liverpool')

        javascript = response.xpath('(.//*[contains(text(),"myLatLng")])[1]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property[@name="lat"]/number/@value').get()
            longitude = selector.xpath('.//property[@name="lng"]/number/@value').get()        
            item_loader.add_value('latitude',latitude)
            item_loader.add_value('longitude',longitude)

        features = " ".join(response.xpath('//h5[contains(text(),"Key Features")]/../ul/li/text()').extract())
        item_loader.add_value('room_count',item_loader.get_output_value('title').split()[0])
        #Bathroom check
        bathroom = re.findall(r'(\d+) bathroom', features)
        if bathroom:
            item_loader.add_xpath('bathroom_count',bathroom[0]) 
        
        #Available date check
        # availability = remove_white_spaces(response.xpath('//*[contains(text(),"Date Available")]/following-sibling::text()').extract_first())
        # if re.search(r'\d{2}/\d{2}/\d{2}',availability):
        #     item_loader.add_value('available_date', format_date(availability, date_format='%d/%m/%y'))

        # EPC check
        epc = response.xpath('//*[contains(text(),"EPC Rating")]/following-sibling::text()').extract_first()
        if epc:
            if epc!=" Pending":
                item_loader.add_value('energy_label', remove_white_spaces(epc))

        #Security deposit check
        deposit = response.xpath('//*[contains(text(),"Security Deposit:")]/following-sibling::text()').extract_first()
        if deposit:
            item_loader.add_value('deposit', str(int(ceil(float(extract_number_only(deposit,thousand_separator=',',scale_separator='.'))))))

        #Furnishing check
        furnishing = remove_white_spaces(response.xpath('//*[contains(text(),"Furnishing")]/following-sibling::text()').extract_first())
        if furnishing:
            # https://www.atlasestateagents.co.uk/property-940/residential/to-let/picton-road-wavertree-l15
            if furnishing == "Furnished":
                item_loader.add_value('furnished', True)
            # https://www.atlasestateagents.co.uk/property-930/residential/to-let/victoria-court-parkfield-road-aigburth-l17
            elif furnishing == "Unfurnished":
                item_loader.add_value('furnished', False)
        
        appliances = response.xpath('//*[contains(text(),"Appliances")]/following-sibling::text()').extract_first()
        if appliances:
            # https://www.atlasestateagents.co.uk/property-655/residential/to-let/aigburth-road-aigburth-l17
            if "washing machine" in appliances.lower():
                item_loader.add_value('washing_machine', True)
            # https://www.atlasestateagents.co.uk/property-655/residential/to-let/aigburth-road-aigburth-l17
            if "dishwasher" in appliances.lower():
                item_loader.add_value('dishwasher', True)
        
        if " parking " in features.lower() or 'garage' in features.lower():
            item_loader.add_value('parking', True)

        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)

        if "swimming" in features.lower() or "swimming pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        
                
        item_loader.add_value('landlord_name','Atlas Estate Agents')
        item_loader.add_value('landlord_phone', '0151 727 2469')
        item_loader.add_value('landlord_email', 'hello@atlasestateagents.co.uk')
        self.position+=1
        item_loader.add_value('position',self.position)
        
        yield item_loader.load_item()