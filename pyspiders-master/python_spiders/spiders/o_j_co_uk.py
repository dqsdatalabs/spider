# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from os import stat
import scrapy
from scrapy.http import headers
from ..loaders import ListingLoader
import json


class OJCoUkSpider(scrapy.Spider):
    name = "o_j_co_uk"
    allowed_domains = ["o-j.co.uk"]
    start_urls = (
        'https://www.o-j.co.uk/api/set/results/grid',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    
    def start_requests(self):
        for url in self.start_urls:
            frmdata = {"sortorder": "price-desc",
                       "RPP": '12',
                       "OrganisationId": "29a932a5-4090-4e5a-bfa2-3d6e45662050",
                       "WebdadiSubTypeName": "Rentals",
                       "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c}",
                       "includeSoldButton": 'true',
                       "incsold": 'true',
                       "page": '1'}
            yield scrapy.FormRequest(url,
                                     method='POST',
                                     formdata=frmdata,
                                     callback=self.parse,
                                     meta={"frmdata": frmdata})

    def parse(self, response, **kwargs):
        listings = response.xpath("//div[contains(@class,'FeaturedProperty')]")
        for item in listings:
            status = item.xpath(".//div[contains(@class,'status')]/span/text()").get()
            url = item.xpath(".//@data-url").get()
            property_url = response.urljoin(url)
            yield scrapy.Request(url=property_url,
                                callback=self.get_property_details,
                                meta={'request_url': property_url})

        frmdata = response.meta.get('frmdata')
        if len(listings) == 12:
            frmdata["page"] = str(int(response.meta.get('frmdata')['page'])+1)
            yield scrapy.FormRequest(response.url,
                                     method='POST',
                                     formdata=frmdata,
                                     callback=self.parse,
                                     meta={"frmdata": frmdata})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get("request_url"))

        external_id = response.meta.get('request_url').split('property/')[1].split('/')[0]
        item_loader.add_value('external_id', external_id)

        item_loader.add_xpath('title', '//h2[@class="color-primary mobile-left"]/text()')

        description = response.xpath('//section[@id="description"]//p/text()').extract_first()
        item_loader.add_value('description', description)
        
        property_type = ""
        if "house" in description.lower() or "house" in response.meta["request_url"].lower():
                property_type = "house"
        elif "studio" in description.lower() or "studio" in response.meta["request_url"].lower():
            property_type = "studio"
        elif "maisonette" in description.lower() or "maisonette" in response.meta["request_url"].lower():
            property_type = "apartment"
        elif "flat" in description.lower() or "flat" in response.meta["request_url"].lower() or "apartment" in description.lower() or "apartment" in response.meta["request_url"].lower():
            property_type = "apartment"
        else: return
        # else:
        #     property_type = ""  
        # property_type = response.meta.get('request_url').split('/')[-2]
        # if property_type not in ["apartment", "house", "studio", "room", "student_apartment"]:
            
            
        item_loader.add_value('property_type', property_type)

        zipcode = response.xpath('//span[@class="displayPostCode"]//text()').extract_first()
        if zipcode:
            zipcode = zipcode.strip()
            item_loader.add_value('zipcode', zipcode)

        address = response.xpath('//span[@class="address1"]//text()').extract_first()
        address = f"{address.strip()} {zipcode}".strip()
        item_loader.add_value('address', address)
        item_loader.add_value("city","London")

        map_cords = response.xpath('//section[@id="maps"]/@data-cords').extract_first()
        map_cords = json.loads(map_cords)
        item_loader.add_value('latitude', map_cords['lat'])
        item_loader.add_value('longitude', map_cords['lng'])

        room_count = response.xpath('.//aside[@id="sidebar"]//img[contains(@src, "bedrooms.svg")]/../span/text()').extract_first()
        if room_count and room_count != "0":
            item_loader.add_value('room_count', room_count)
        elif room_count and room_count == "0" and property_type == "studio":
            item_loader.add_value('room_count', "1")
        item_loader.add_xpath('bathroom_count', './/aside[@id="sidebar"]//img[contains(@src, "bathrooms.svg")]/../span/text()')

        epc_string = response.xpath('//li[contains(text(),"EPC")]/text()').extract_first()
        if epc_string:
            epc = epc_string.split('EPC Rating ')[-1]
            if len(epc) == 1 and epc.isalpha():
                item_loader.add_value('energy_label', epc)

        features = response.xpath('//ul[@class="color-white"]//text()').extract()
        if features:
            featuresString = " ".join(features)

            # https://www.o-j.co.uk/property/101841012195/se10/tarves-way/apartment/2-bedrooms
            if "parking" in featuresString.lower(): 
                item_loader.add_value('parking', True)

            if "elevator" in featuresString.lower() or 'lift' in featuresString.lower(): 
                item_loader.add_value('elevator', True)

            # https://www.o-j.co.uk/property/101841012195/se10/tarves-way/apartment/2-bedrooms
            if "balcony" in featuresString.lower(): 
                item_loader.add_value('balcony', True)

            # https://www.o-j.co.uk/property/101841009789/se8/lower-road/apartment/2-bedrooms
            if "terrace" in featuresString.lower(): 
                item_loader.add_value('terrace', True)

            # https://www.o-j.co.uk/property/101841012278/e3/fairfield-road/apartment/1-bedroom
            if "swimming pool" in featuresString.lower():
                item_loader.add_value('swimming_pool', True)

            if "washing machine" in featuresString.lower():
                item_loader.add_value('washing_machine', True)

            if "dishwasher" in featuresString.lower() or "washer" in featuresString.lower():
                item_loader.add_value('dishwasher', True)
    
            if " furnished" in featuresString.lower(): 
                item_loader.add_value('furnished', True)
            elif "unfurnished" in featuresString.lower() or "un-furnished" in featuresString.lower(): 
                item_loader.add_value('furnished', False)

            if "pets considered" in featuresString.lower(): 
                item_loader.add_value('pets_allowed', True)

        rent_value = response.xpath('//span[@class="nativecurrencyvalue"]//text()').extract_first()
        currency = response.xpath('//span[@class="nativecurrencysymbol"]//text()').extract_first()
        rent_string = currency+rent_value
        item_loader.add_value('rent_string', rent_string)

        item_loader.add_xpath('images', '//div[@class="img-gallery"]//div/@data-bg')
        item_loader.add_xpath('floor_plan_images', '//h2[text()="Floorplans"]/following-sibling::figure/img/@data-src')

        item_loader.add_value('landlord_name', "Oliver Jaques")
        item_loader.add_xpath('landlord_email', '//span[@class="fa fa-envelope"]/following-sibling::a/text()')
        item_loader.add_xpath('landlord_phone', '//i[@class="fa fa-phone"]/following-sibling::a[1]/text()')

        self.position += 1
        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", "OJ_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
