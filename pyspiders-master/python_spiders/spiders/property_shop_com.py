# -*- coding: utf-8 -*-
# Author: Nipun Arora
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
import lxml 
import js2xml

class MySpider(Spider):

    name = "property_shop_com"
    allowed_domains = ['rightmove.co.uk']
    start_urls = ['https://www.rightmove.co.uk/']
    execution_type = 'testing'
    country ='united_kingdom'
    locale ='en'
    position = 0
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=BRANCH%5E85046&propertyTypes=flat&primaryDisplayPropertyType=flats&includeLetAgreed=true&mustHave=&dontShow=&furnishTypes=&keywords=",
                "prop_type":"apartment"
            },
            {
                "url": "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=BRANCH%5E85046&propertyTypes=bungalow%2Cdetached%2Csemi-detached%2Cterraced&includeLetAgreed=true&mustHave=&dontShow=&furnishTypes=&keywords=",
                "prop_type":"house"
            },
        ]
        for urls in start_urls:
            yield Request(url=urls.get("url"),
                                callback=self.parse,
                                meta={'request_url': urls.get("url"),
                                'prop_type' : urls.get("prop_type"),
                                'page':0})

    def parse(self, response, **kwargs):

        listings = response.xpath('//div[@class="propertyCard-details"]/a/@href').extract()
        listings = [listing for listing in listings if len(listing)>0]
        for url in listings:
            url = response.urljoin(url)
            yield Request(
                url=url,
                callback=self.get_property_details,
                meta={'request_url': url, "prop_type":response.meta["prop_type"]})
        
        if len(listings) == 24:
            next_page_url = response.meta.get('request_url').split('&index=')[0] + '&index=' + str(response.meta.get('page') + 24)
            yield Request(
                    url=next_page_url,
                    callback=self.parse,
                    meta={
                    'request_url':next_page_url,
                    "prop_type":response.meta["prop_type"],
                    'page':response.meta.get('page') + 24})

    def get_property_details(self, response):

        address = response.xpath('//h1[@class="_2uQQ3SV0eMHL1P6t5ZDo2q"]/text()').extract_first()
        rent_string = response.xpath('//div[@class="_1gfnqJ3Vtd1z40MlC0MzXu"]//span/text()').extract_first()
        if rent_string:
            rent_string = rent_string.replace('pcm', '')
        
        features = ", ".join(response.xpath('//h2[contains(text(), "Key features")]/..//li/text()').extract())
        description = " ".join(response.xpath('//h2[contains(text(), "Property description")]/../div/div/text()').extract())
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_source', "PropertyShop_PySpider_united_kingdom_en")
        item_loader.add_value('external_link', response.meta.get('request_url'))
        #item_loader.add_value('external_id', str(extract_number_only(response.meta.get('request_url'))))
        item_loader.add_value('property_type', response.meta["prop_type"])
        item_loader.add_xpath('images', '//img[@data-object-fit="cover"]/@src')
        item_loader.add_value('address', address)
        item_loader.add_value('title', address)
        item_loader.add_xpath('rent_string', '//div[@class="_1gfnqJ3Vtd1z40MlC0MzXu"]//span/text()')
        
        item_loader.add_value('description', description)

        if address:
            city,zipcode = address.split(', ')[-2], address.split(', ')[-1]
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)

        javascript = response.xpath('.//script[contains(text(), "latitude")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            item_loader.add_value('latitude', xml_selector.xpath('.//property[@name="latitude"]/number/@value').extract_first())
            item_loader.add_value('longitude', xml_selector.xpath('.//property[@name="longitude"]/number/@value').extract_first())

        # geolocator = Nominatim(user_agent=random_user_agent())
        # location = geolocator.geocode(address)
        # if location:
        #     item_loader.add_value('latitude', str(location.latitude))
        #     item_loader.add_value('longitude', str(location.longitude))

        landlord_phone = response.xpath("//script[contains(.,'localNumber')]/text()").get()
        if landlord_phone: item_loader.add_value("landlord_phone", landlord_phone.split('localNumber":"')[-1].split('"')[0].strip())
        item_loader.add_value("landlord_name", "Property Shop Sales & Lettings")
        item_loader.add_value('landlord_email', 'lettings@propertyshopltd.com')

        # https://www.rightmove.co.uk/property-to-rent/property-74255721.html
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)
        if "balony" in features.lower():
            item_loader.add_value('balcony', True)
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)
        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)
        # https://www.rightmove.co.uk/property-to-rent/property-73640625.html
        if "parking" in features.lower() or "garage" in features.lower():
            item_loader.add_value('parking', True)
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        if "furnished" in features.lower():
            if "unfurnished" in features.lower():
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//dt[contains(.,'available date')]/following-sibling::dd/text()", input_type="F_XPATH")

        script_data = response.xpath("//script[contains(.,'PAGE_MODEL')]/text()").get()
        if script_data:
            data = json.loads(script_data.split("PAGE_MODEL = ")[1].strip())["propertyData"]

            if data and "id" in data:
                item_loader.add_value("external_id", data["id"])
            
            if data and "location" in data:
                if "latitude" in data["location"] and "longitude" in data["location"]:
                    item_loader.add_value("latitude", str(data["location"]["latitude"]))
                    item_loader.add_value("longitude", str(data["location"]["longitude"]))
            
            if data and "bedrooms" in data:
                item_loader.add_value("room_count", str(data["bedrooms"]))
            else:
                room_count = response.xpath("//div[.='BEDROOMS']/..//div[@class='_1fcftXUEbWfJOJzIUeIHKt']/text()").get()
                if room_count:
                    item_loader.add_value("room_count", room_count.replace("x", "").strip())
                else:
                    room_count = response.xpath('//div[contains(text(), "BEDROOMS")]/../div[2]//text()').extract_first()
                    if room_count:
                        item_loader.add_value('room_count', room_count.replace("x", "").strip())
            
            if data and "bathrooms" in data:
                item_loader.add_value("bathroom_count", str(data["bathrooms"]))
            else:
                bathroom_count = response.xpath("//div[.='BATHROOMS']/..//div[@class='_1fcftXUEbWfJOJzIUeIHKt']/text()").get()
                if bathroom_count:
                    item_loader.add_value("bathroom_count", bathroom_count.replace("x", "").strip())
                else:
                    bathroom_count = response.xpath('//div[contains(text(), "BATHROOMS")]/../div[2]//text()').extract_first()
                    if bathroom_count:
                        item_loader.add_value('bathroom_count', bathroom_count.replace("x", "").strip())
            
            # if data and "lettings" in data:
            #     if "letAvailableDate" in data["lettings"]:
            #         available_date = data["lettings"]["letAvailableDate"]
            #     else:
            #         available_date = response.xpath("//span[contains(.,'Let available date')]/following-sibling::*/text()").get()
            #     if available_date:
            #         date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            #         date2 = date_parsed.strftime("%Y-%m-%d")
            #         item_loader.add_value("available_date", date2)
            
        if not item_loader.get_collected_values("room_count"):
            room_count = response.xpath("//div[contains(text(),'BEDROOMS')]/following-sibling::div/div[last()]/div/text()").get()
            room_count = "".join(filter(str.isnumeric, room_count)) if room_count else None
            item_loader.add_value('room_count', room_count)
            
        
        if not item_loader.get_collected_values("bathroom_count"):
            bathroom_count = response.xpath("//div[contains(text(),'BATHROOMS')]/following-sibling::div/div[last()]/div/text()").get()
            bathroom_count = "".join(filter(str.isnumeric, bathroom_count)) if bathroom_count else None
            item_loader.add_value('bathroom_count', bathroom_count)


        self.position += 1
        item_loader.add_value('position', self.position)

        yield item_loader.load_item()