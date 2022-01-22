# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
import json
import re
from ..helper import format_date
from scrapy import Request


class VallettapropertiesSpider(scrapy.Spider):
    name = 'valletaproperties_co_uk'
    allowed_domains = ["vallettaproperties.co.uk"]
    # property_type not set here in meta contents since url remains same on changing property type in filters
    start_urls = (
        'https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=',
        ''
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = "Vallettapropeties_PySpider_united_kingdom_en"
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=87&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=88&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=92&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=107&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=4&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=6&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=7&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=11&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=19&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=21&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=106&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=11&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=",
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.vallettaproperties.co.uk/student?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=&minPrice=&maxPrice=&minBedrooms=&maxBedrooms="
                ],
                "property_type": "student_apartment"
            },
            {
                "url": [
                    "https://www.vallettaproperties.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=89&minPrice=&maxPrice=&minBedrooms=&maxBedrooms="
                ],
                "property_type": "studio"
            }
            
            
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    def parse(self, response, **kwargs):
        page = response.meta.get('page', 2)
        seen = False
        listings = response.xpath('//div[@class="card"]')
        for property_item in listings:
            property_url = property_item.xpath('./a/@href').extract_first()
            property_url = response.urljoin(property_url)
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url, 'property_type': response.meta.get('property_type')}
            )
            seen = True
            
        if response.meta.get('property_type') == "student_apartment":
            if seen:
                f_url = f"https://www.vallettaproperties.co.uk/student?page={page}&view=grid&distance=6"
                yield scrapy.Request(
                    url=response.urljoin(f_url),
                    callback=self.parse,
                    meta={'property_type': response.meta.get('property_type'), "page": page+1}
                )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        external_link = response.meta.get('request_url')
        item_loader.add_value("external_link", external_link)

        item_loader.add_xpath('title', '//h1/text()')
        address = response.xpath("//h2/text()").get()
        if address:
            item_loader.add_value("address", address)

        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('city', "Leeds")

        # map_coordinates = json.loads(response.xpath('//*[@class="map google-map"]/@data-map-options').extract()[0])['items'][0]['latlng']
        # item_loader.add_value("latitude", map_coordinates[0])
        # item_loader.add_value("longitude", map_coordinates[1])

        
        room_count = response.xpath("//h1/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
            
        bathroom_count = response.xpath("//div[@id='tab-details']/p[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("bathroom")[0].strip().split(" ")[-1]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)

        imagesLinks = [x for x in response.xpath("//div[@class='carousel-slide']//@data-lazy-img").extract()]
        if imagesLinks:
            item_loader.add_value('images', imagesLinks)

        rent = "".join(response.xpath("//p[@class='property-price']/text()").extract())
        rentPeriod = response.xpath("//p[@class='property-price']/small/text()").extract_first()
        if rentPeriod == 'pppw' or rentPeriod == 'pw' or "week" in rentPeriod.lower():
            rent = rent.replace("£","").replace(",","").strip()
            rent = int(rent)*4
            item_loader.add_value("rent", rent)
        else:
            rent = rent.replace("£","").replace(",","").strip()
            item_loader.add_value("rent", rent)

        item_loader.add_value("currency", "GBP")
        
        desc = "".join(response.xpath("//div[@id='tab-details']//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        features = response.xpath("//div[@id='tab-details']//text()").extract()
        if features:
            featuresString = " ".join(features)

            # http://www.vallettaproperties.co.uk/property-to-rent/west-yorkshire/leeds-oakwood/semi-detached-bungalow/57484/roundhay-road-2            
            if "parking" in featuresString.lower(): 
                item_loader.add_value('parking', True)

            if "elevator" in featuresString.lower() or 'lift' in featuresString.lower(): 
                item_loader.add_value('elevator', True)

            if "balcony" in featuresString.lower(): 
                item_loader.add_value('balcony', True)

            if "terrace" in featuresString.lower(): 
                item_loader.add_value('terrace', True)

            if "swimming pool" in featuresString.lower():
                item_loader.add_value('swimming_pool', True)

            if "washing machine" in featuresString.lower():
                item_loader.add_value('washing_machine', True)

            if "dishwasher" in featuresString.lower() or "washer" in featuresString.lower():
                item_loader.add_value('dishwasher', True)
    
            # http://www.vallettaproperties.co.uk/property-to-rent/west-yorkshire/leeds-woodhouse/flat-6/apartment/65304/st-johns-terrace-2
            if " furnished" in featuresString.lower(): 
                item_loader.add_value('furnished', True)
            # http://www.vallettaproperties.co.uk/property-to-rent/west-yorkshire/leeds-alwoodley/semi-detached-property/62281/primley-park-close
            elif "unfurnished" in featuresString.lower() or "un-furnished" in featuresString.lower(): 
                item_loader.add_value('furnished', False)

            if "pets considered" in featuresString.lower(): 
                item_loader.add_value('pets_allowed', True)

        landlord_name = "Valletta Properties"
        landlord_email = "info@vallettapropeties.co.uk"
        landlord_phone = "01132899888"
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_value('landlord_email', landlord_email)
        item_loader.add_value('landlord_phone', landlord_phone)

        item_loader.add_value("external_source", self.external_source)
        
        yield item_loader.load_item()