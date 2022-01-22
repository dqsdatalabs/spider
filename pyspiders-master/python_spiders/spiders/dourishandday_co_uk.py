# Author: Madhumitha S
# Team: Sabertooth

import re
import scrapy
import lxml.etree
import js2xml
from scrapy import Selector
from ..loaders import ListingLoader
from ..helper import remove_white_spaces


class DourishanddayCoUkSpider(scrapy.Spider):
    name = 'dourishandday_co_uk'
    allowed_domains = ['dourishandday.co.uk']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position=0

    def start_requests(self):
        start_urls = [
            'https://www.dourishandday.co.uk/properties-to-let'
        ]

        for url in start_urls:
            yield scrapy.Request(
                url=url, 
                callback=self.parse,
                )

    def parse(self, response, **kwargs):
       
        listings = response.xpath('//div[@class="columnProps"]/ul/li')
        for property_item in listings:
            
            property_url = f"https://www.dourishandday.co.uk{property_item.xpath('.//ul/li/a/@href').extract_first()}"
                
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url}
                )
        next_page_url=response.xpath('//a[contains(@title,"Next")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=f'https://www.dourishandday.co.uk{next_page_url}',
                callback=self.parse
             )

    def get_property_details(self, response):
        sale_type = response.xpath('//b[contains(text(),"Sale Type")]/../text()').extract_first()
        if 'let stc' in sale_type.lower():
            return
        item_loader = ListingLoader(response=response)        
        item_loader.add_value("external_link", response.meta.get('request_url'))
        external_id = response.xpath('.//b[contains(text(),"Ref")]/../text()').extract_first()
        item_loader.add_value('external_id', external_id.split(': ')[1])
        item_loader.add_xpath('title', '//div[contains(@class,"span8 ")]/h2/text()')
        bedrooms = remove_white_spaces(response.xpath('.//i[contains(@class,"bedroom")]/following-sibling::text()').extract_first())
        item_loader.add_value('room_count', bedrooms)
        bathrooms = remove_white_spaces(response.xpath('//i[contains(@class,"bathroom")]/following-sibling::text()').extract_first())
        item_loader.add_value('bathroom_count', bathrooms)
        street = response.xpath('.//address/strong/text()').extract_first()
        area_zip = response.xpath('.//address/br/following-sibling::text()').extract_first()
        if len(area_zip.split()) == 3:
            city = area_zip.split()[0]
            zipcode = " ".join(area_zip.split()[1:3])
        elif len(area_zip.split()) == 4:
            city = " ".join(area_zip.split()[0:2])
            zipcode = " ".join(area_zip.split()[2:4])
        county = response.xpath('.//b[contains(text(),"County")]/../text()').extract_first()
      
        if county:  
            county=county.split(': ')[1]     
            address = street + ', ' + city + ', ' + county + ', ' + zipcode
        elif not county:
            address = street + ', ' + city + ', ' + zipcode
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('address', address)
        rent_strings = response.xpath('.//small[contains(@class,"price")]/text()').extract_first()
        rent_string = re.findall(r'£\d+,?\d+', rent_strings)
        if rent_string:
            item_loader.add_value('rent_string', rent_string[0])
        description = "".join(response.xpath('.//div[contains(@class,"span12 eapow-desc")]/p/text()').extract())
        item_loader.add_value('description', description)
        item_loader.add_xpath('images', './/li[@class="galleryThumb"]/a/@href')
        item_loader.add_xpath('floor_plan_images', './/a[contains(@data-lightbox,"floorplan")]/@href')
                
        javascript = response.xpath('//script[contains(text(),"eapowmapoptions")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//var[@name="eapowmapoptions"]//property[@name="lat"]/string/text()').extract_first()
            longitude = selector.xpath('.//var[@name="eapowmapoptions"]//property[@name="lon"]/string/text()').extract_first()
            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)

        features = "".join(response.xpath('.//ul[@id="starItem"]/li/text()').extract())
        
        apartment_types = ["apartment", "flat", "penthouse", "duplex", "dakappartement", "triplex", 'bungalow']
        house_types = ['bungalow', 'maison', 'house', 'home', 'cottage', 'detached house', 'house terrace',
                    'terrace', 'maisonette', 'mid terrace', 'semi-detached', 'detached', 'mid terraced house', 'property']
        studio_types = ["studio", 'studio flat']
        if any(i in description.lower() or i in features.lower() for i in studio_types):
            property_type = "studio"
        elif any(i in description.lower() or i in features.lower() for i in apartment_types):
            property_type = "apartment"
        elif any(i in description.lower() or i in features.lower() for i in house_types):
            property_type = "house"
        if property_type:
            item_loader.add_value('property_type', property_type)
        
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)

        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)
        
        if " parking " in features.lower() or 'garage' in features.lower():
            item_loader.add_value('parking', True)

        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        # https://www.dourishandday.co.uk/properties-to-let/property/9926482-castle-bank-stafford
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)

        if "swimming" in features.lower() or "swimming pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)
        
        # https://www.dourishandday.co.uk/properties-to-let/property/8296868-47-newport-road-stafford
        if " furnished" in features.lower():
            item_loader.add_value('furnished', True)
        
        # https://www.dourishandday.co.uk/properties-to-let/property/9267118-austin-friars-stafford
        if 'no pets' in features.lower():
            item_loader.add_value('pets_allowed', False)

        #Floor check
        floor = re.findall(r' ([a-z]* floor)', features.lower())
        if floor:
            item_loader.add_value('floor', floor[0])

        item_loader.add_value('landlord_name', 'Dourish & Day')        
        item_loader.add_value('landlord_email', 'hello@dourishandday.co.uk')
        item_loader.add_value('landlord_phone', '01785 223344')
        self.position += 1
        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", "Dourishandday_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
