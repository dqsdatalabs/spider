# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import scrapy
from ..helper import extract_number_only, format_date,remove_white_spaces
from ..loaders import ListingLoader
import js2xml
import lxml
from scrapy import Selector


class MasonknightpropertiesComSpider(scrapy.Spider):
    name = "masonknightproperties_com"
    allowed_domains = ["masonknightproperties.com"]
    start_urls = [
        {'url':'https://masonknightproperties.com/property-search/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=22&minimum_floor_area=&maximum_floor_area=&commercial_property_type=',
        'property_type':'apartment'},
        {'url':'https://masonknightproperties.com/property-search/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=9&minimum_floor_area=&maximum_floor_area=&commercial_property_type=',
        'property_type':'house'},
        {'url':'https://masonknightproperties.com/property-search/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=18&minimum_floor_area=&maximum_floor_area=&commercial_property_type=',
        'property_type':'house'},
    ]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position=0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url.get('url'), callback=self.parse,
                                    meta={'property_type':url.get('property_type')})

    def parse(self, response, **kwargs):
        listings=response.xpath('//li[contains(@class,"availability-to-let")]//a/@href').extract()
        listings=set(listings)
        for property_url in listings:
            property_url=response.urljoin(property_url)
            yield scrapy.Request(url=property_url,
                                callback=self.get_property_details,
                                meta={'request_url':property_url,'property_type':response.meta.get('property_type')})

        next_page_url=response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_page_url:
            next_page_url=response.urljoin(next_page_url)
            yield response.follow(
                url=next_page_url,
                callback=self.parse,
                meta={'property_type':response.meta.get('property_type')})
                            
    def get_property_details(self,response):
        item_loader = ListingLoader(response=response)

        property_type=response.meta.get('property_type')
        item_loader.add_value('property_type',property_type)
        rented = response.xpath("//div[@class='images lazyload']/div[contains(.,'Let Agreed')]/text()").extract_first()
        if rented:
            return
        external_id=response.xpath('//span[contains(text(),"Ref")]/following-sibling::text()').extract_first()
        if external_id:
            external_id=external_id.strip()
            item_loader.add_value('external_id',external_id)

        title = response.xpath("//h1/text()").get()
        item_loader.add_value('title', title)

        description="".join(response.xpath('//div[contains(@class,"summary-contents")]/text()').extract())
        if description:
            description=remove_white_spaces(description)
            item_loader.add_value('description',description)
            
        address=response.xpath('//h1[@class="property_title entry-title"]/text()').extract_first()
        if address:

            zipcode=address.split(", ")[-1]
            if re.match(r'[A-Z]{2}[0-9]\s[0-9][A-Z]{2}',zipcode):
                item_loader.add_value('zipcode',zipcode)

            address_str = re.search(r'(?<=\s(in|on)\s)(.+?)(?=\s\d{4}-\d{4})',address)

            if address_str:
                address_str=address_str.group()

            elif "road" in address.lower():
                address_road=address.lower().split("road")[0].split()[-1]
                address_str=address_road+" road " + ' '.join(address.lower().split("road")[1].split()[:-1])


            elif "lane" in address.lower():
                address_lane=address.lower().split("lane")[0].split()[-1]
                address_str=address_lane+" lane " + ' '.join(address.lower().split("lane")[1].split()[:-1])

            if address_str:
                address_str=address_str.replace('year','').replace('Year','').replace('  ',' ').replace('academic','')
                if address_str[-1]==',':
                        address_str = address_str[:-1]
                item_loader.add_value('address',address_str)
            else:
                if title.count(",") >1:
                    address = title.split(",")[-2].strip()
                    zipcode = title.split(",")[-1].strip()
                    item_loader.add_value("address", address)
                    item_loader.add_value("city", address)
                elif title.count(",") ==1:
                    address = title.split(",")[-1].strip()
                    item_loader.add_value("address", address)
                else:
                    item_loader.add_value("address", title.split(" ")[-1])

        javascript = response.xpath('.//script[contains(text(), "google.maps.LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            coordinates=xml_selector.xpath('//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()
            item_loader.add_value('latitude',coordinates[0])
            item_loader.add_value('longitude',coordinates[1])

        room_count=response.xpath('//span[contains(text(),"Bedroom")]/following-sibling::text()').extract_first()
        if room_count:
            room_count=room_count.strip()
            item_loader.add_value('room_count',room_count)
        
        bathroom_count=response.xpath('//span[contains(text(),"Bathroom")]/following-sibling::text()').extract_first()
        if bathroom_count:
            bathroom_count=bathroom_count.strip()
            item_loader.add_value('bathroom_count',bathroom_count)


        rent= "".join(response.xpath('//div[contains(@class,"price")]/text()').extract_first())
        if rent:
            if "pw" in rent:
                rent = rent.strip().split(" ")[0].replace("£","")
                item_loader.add_value("rent", int(float(rent))*4)
            else:
                rent = rent.strip().split(" ")[0].replace("£","").replace(",","")
                item_loader.add_value("rent", rent)
                
        item_loader.add_value("currency", "GBP")
            
        features=response.xpath('//div[@class="features"]//text()').extract()
        if features:
            featuresString=" ".join(features)

            if "elevator" in featuresString.lower() or 'lift' in featuresString.lower(): 
                item_loader.add_value('elevator',True)

            if "balcony" in featuresString.lower(): 
                item_loader.add_value('balcony',True)

            if "terrace" in featuresString.lower(): 
                item_loader.add_value('terrace',True)

            if "swimming pool" in featuresString.lower():
                item_loader.add_value('swimming_pool',True)

            if "washing machine" in featuresString.lower():
                item_loader.add_value('washing_machine',True)

            if "dishwasher" in featuresString.lower() or "washer" in featuresString.lower():
                item_loader.add_value('dishwasher',True)

            if "pets considered" in featuresString.lower(): 
                item_loader.add_value('pets_allowed', True)

        furnished=response.xpath('//span[contains(text(),"Furnished")]/following-sibling::text()').extract_first()
        # https://masonknightproperties.com/property/a-fantastic-opportunity-to-acquire-a-new-5-bedroom-all-en-suite-property-in-harborne/
        if furnished:
            if furnished.lower().strip()=='furnished':
                item_loader.add_value('furnished',True)
            elif "unfurnished" in furnished.lower().strip() or "un-furnished" in furnished.lower().strip():
                item_loader.add_value('furnished',False)

        # https://masonknightproperties.com/property/a-fantastic-opportunity-to-acquire-a-new-5-bedroom-all-en-suite-property-in-harborne/
        parking=response.xpath('//span[contains(text(),"Parking")]/following-sibling::text()').extract_first()
        if parking:
            item_loader.add_value('parking',True)

        # https://masonknightproperties.com/property/a-fantastic-opportunity-to-acquire-a-new-5-bedroom-all-en-suite-property-in-harborne/
        terrace_check = response.xpath('//span[contains(text(),"Type")]/following-sibling::text()').extract_first()
        if 'terrace' in terrace_check.lower():
            item_loader.add_value("terrace", True)

        available_date_string=response.xpath('//span[contains(text(),"Available")]/following-sibling::text()').extract_first()
        if available_date_string:
            available_date_string=available_date_string.strip()
            available_date_string=available_date_string.replace('st','').replace('rd','').replace('nd','').replace('th','')
            available_date_string=available_date_string.replace('Augu','August')
            available_date=format_date(available_date_string,'%d %B %Y')
            item_loader.add_value('available_date',available_date)

        deposit=response.xpath('//span[contains(text(),"Deposit")]/following-sibling::text()').extract_first()
        if deposit:
            deposit=extract_number_only(deposit,',','.')
            item_loader.add_value('deposit',deposit)

        images=response.xpath('//a[@class="propertyhive-main-image"]/@href').extract()
        if images:
            images=list(set(images))
            item_loader.add_value('images',images)

        item_loader.add_xpath('floor_plan_images','//li[@class="action-floorplans"]/a/@href')

        item_loader.add_value('landlord_name',"Mason Knight Properties")
        item_loader.add_value('landlord_phone','0121 472 5897')
        item_loader.add_value('landlord_email','contact@masonknightproperties.com')
        item_loader.add_value("external_source", "Masonknightproperties_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value("external_link",response.meta.get("request_url"))

        self.position+=1
        item_loader.add_value("position",self.position)
        yield item_loader.load_item()
