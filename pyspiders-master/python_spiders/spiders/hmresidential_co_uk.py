# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, remove_white_spaces, format_date
from geopy.geocoders import Nominatim
import re
import js2xml
import lxml.etree
from parsel import Selector
from datetime import date
from ..user_agents import random_user_agent


class HmresisentialSpider(scrapy.Spider):
    name = "hmresidential_co_uk"
    allowed_domains = ["hmresidential.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    listing_new = []
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=upstairs-flat&estate_property_price&estate_property_bedrooms&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D#038',
                'property_type': 'apartment'},
            {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=apartment&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
                'property_type': 'apartment'},
            {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=flat&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
                'property_type': 'apartment'},
            {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=ground-floor-flat&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
                'property_type': 'apartment'},
            {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=upper-floor-flat&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
                'property_type': 'apartment'},
            {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=upper-flat&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
                'property_type': 'apartment'},
            {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=house&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
                'property_type': 'house'},
            {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=maisonette&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
                'property_type': 'house'},
            {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=mid-terraced-house&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
                'property_type': 'house'},
            {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=semi-detached-house&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
                'property_type': 'house'},
            # # property extracted from text
            # {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=shared-accommodation&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
            #     'property_type': ''},
            # # property extracted from text
            # {'url': 'https://hmresidential.co.uk/property-map/page/1/?estate_property_location=all&estate_property_type=all&estate_property_price=&estate_property_bedrooms=&estate_property_status=lettings&paytype=1&pageid=206&searchid=W2N1c3RvbV9wcm9wZXJ0eV9zZWFyY2hfZm9ybSBsb2NhdGlvbj0iQXJlYSIgdHlwZT0iUHJvcGVydHkgVHlwZSIgcHJpY2U9IlByaWNlIiBiZWRyb29tcz0iQmVkcm9vbXMiXQ%3D%3D',
            #     'property_type': ''}
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')},
                                 dont_filter=True)

    def parse(self, response, **kwargs):
        listing = response.xpath('.//*[contains(@class,"property-item border-box")]/a/@href').extract()
        # 4 properties getting excluded if only used urls according to the proeprty_type
        # https://hmresidential.co.uk/property/chillingham-road-ne6-5bu/
        # https://hmresidential.co.uk/property/railway-terrace/
        # https://hmresidential.co.uk/property/flat-1-breamish-street/
        # https://hmresidential.co.uk/property/hillsleigh-road-cowgate/
        for property_url in listing:
            if property_url not in self.listing_new:
                self.listing_new.append(property_url)
                yield scrapy.Request(
                    url=response.urljoin(property_url),
                    callback=self.get_property_details,
                    meta={'request_url': response.urljoin(property_url),
                        "property_type": response.meta["property_type"]}
                )

        if len(response.xpath('.//*[contains(@class,"property-item border-box")]/a')) > 0:
            current_page = re.findall(r"(?<=page/)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page/)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta["property_type"]},
                dont_filter=True
            )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        
        external_id = response.xpath('.//*[contains(@title,"Id")]/text()').extract_first()
        title = response.xpath('.//*[@class="title"]//span/text()').extract_first()

        item_loader.add_xpath('description', './/*[@id="property-content"]//p/text()')

        property_type = response.meta.get('property_type')
        if len(property_type) != 0:
            item_loader.add_value('property_type', property_type)
        else:
            apartment_types = ["appartement", "apartment", "flat",
                               "penthouse", "duplex", "triplex", "residential complex"]
            house_types = ['chalet', 'bungalow', 'maison', 'house', 'home']
            studio_types = ["studio"]

            text = item_loader.get_output_value('description').lower()
            if any(word in text.lower() for word in apartment_types + house_types + studio_types):
                if any(i in text.lower() for i in studio_types):
                    item_loader.add_value('property_type', 'studio')
                elif any(i in text.lower() for i in apartment_types):
                    item_loader.add_value('property_type', 'apartment')
                elif any(i in text.lower() for i in house_types):
                    item_loader.add_value('property_type', 'house')

        item_loader.add_value("external_link", response.meta["request_url"])

        room_count = response.xpath('.//*[contains(@title,"Bedroom")]/text()').extract_first()
        if extract_number_only(room_count) == 0:
            return
        bathroom_count = response.xpath('.//*[contains(@title,"Bathroom")]/text()').extract_first()
        item_loader.add_value('room_count', extract_number_only(room_count))
        item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))

        item_loader.add_value('external_id', remove_white_spaces(external_id))
        item_loader.add_xpath('images', './/*[@class="property-image  zoom"]/@data-image')
        item_loader.add_value('title', remove_white_spaces(title))
        item_loader.add_xpath('rent_string', './/*[@class="meta"]/text()')

        available_date=response.xpath( "//section[@id='property-content']//p[contains(.,'AVAILABLE')]//text()").get()
        if available_date:
            item_loader.add_value('available_date', available_date.split("AVAILABLE")[1])
        else:
            available_date=response.xpath( "//section[@id='property-content']//p[contains(.,'Available')]//text()").get()
            if available_date:
                item_loader.add_value('available_date', available_date.split("Available"))
        
        furnished=response.xpath('//section[@id="property-content"]//p//text()[contains(., "Furnished")]').get()
        if furnished:
            item_loader.add_value('furnished', True)

        features = ' '.join(response.xpath('.//h3/span[contains(text(),"Features")]/../following-sibling::ul/li//text()').extract())

        if 'terrace' in title.lower() or 'terrace' in features.lower():
            item_loader.add_value('terrace', True)

        if 'balcony' in features.lower():
            item_loader.add_value('balcony', True)

        if 'washing machine' in features.lower():
            item_loader.add_value('washing_machine', True)

        if 'elevator' in features.lower() or 'lift' in features.lower():
            item_loader.add_value('elevator', True)
        
        if 'swimming pool' in features.lower():
            item_loader.add_value('swimming_pool', True)

        if 'dishwasher' in features.lower():
            item_loader.add_value('dishwasher', True)

        if 'furnished' in features.lower():
            item_loader.add_value('furnished', True)

        # https://hmresidential.co.uk/property/baltic-quay-mill-road/
        if 'parking' in features.lower():
            item_loader.add_value('parking', True)
        
        # https://hmresidential.co.uk/property/flat-1-breamish-street/
        if ' furnished' in features.lower():
            item_loader.add_value('furnished', True)
        elif 'unfurnished' in features.lower():
            item_loader.add_value('furnished', False)

        address = "".join(response.xpath("//h1[@class='title']//text()").getall())
        if address:
            if "–" in address:
                address = address.split("–")[0].strip()
            elif "£" in address:
                address = address.split("£")[0].strip()
            elif "." in address:
                address = address.split(".")[0].strip()
            elif "ALL" in address:
                address = address.split("ALL")[0].strip()
            elif "&" in address:
                address = ""
            else:
                address = address
            
            if address:
                if "SUMMER" in address:
                    address = address.split("SUMMER")[0].strip()
                address = address.strip(",")
                item_loader.add_value("address", address.strip())
                
                zipcode = address.split()[-2:]
                zipcode = ' '.join(zip for zip in zipcode if any(z for z in zip if z.isdigit()))     
                if zipcode:
                    item_loader.add_value('zipcode', zipcode)
                
                city = address.split(',')
                if len(city) > 2:
                    city = city[-1].strip(',').strip()
                    if city in zipcode:
                        city = ' '.join(address.split()[:2]).strip(',')
                else:
                    city = city[0]

                if city.count(' ') >= 1:
                    city = city.split()[0]    
                if city:
                    item_loader.add_value('city', city.strip())    
                
        javascript = response.xpath('.//script[contains(text(),"mapOptions")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            lat_lng = selector.xpath('.//identifier[@name="LatLng"]/../../..//arguments/number/@value').extract()
            if lat_lng:
                lat_lng = lat_lng[:2]
                item_loader.add_value('latitude', lat_lng[0])
                item_loader.add_value('longitude', lat_lng[1])

        item_loader.add_value('landlord_name', 'HM Residential')
        item_loader.add_value('landlord_phone', '0191 272 7700')
        item_loader.add_value('landlord_email', 'info@hmresidential.co.uk')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Hmresidential_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
