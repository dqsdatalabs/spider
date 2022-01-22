# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_rent_currency, format_date, extract_number_only, remove_white_spaces
from datetime import date
import lxml
import js2xml
from scrapy import Selector


class SandersonJamesCoUk(scrapy.Spider):
    name = "sandersonjames_co_uk"
    allowed_domains = ['sandersonjames.co.uk']
    start_urls = ['https://sandersonjames.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    position = 0
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        start_url = [
            {
                'url': 'https://sandersonjames.co.uk/index.php?option=com_propertylab&layout=propertysearch&task=propertysearch&start=0&perpage=20&minprice=1&type=&type=tolet&address3=&prop_type=Apartment&minbeds=0&minprice=&maxprice=',
                'property_type': 'apartment'
            },
            {
                'url': 'https://sandersonjames.co.uk/index.php?option=com_propertylab&layout=propertysearch&task=propertysearch&start=0&perpage=20&minprice=1&type=&type=tolet&address3=&prop_type=Bungalow&minbeds=0&minprice=&maxprice=',
                'property_type': 'house'
            },
            {
                'url': 'https://sandersonjames.co.uk/index.php?option=com_propertylab&layout=propertysearch&task=propertysearch&start=0&perpage=20&minprice=1&type=&type=tolet&address3=&prop_type=Student+Property&minbeds=0&minprice=&maxprice=',
                'property_type': 'apartment'
            },
            {
                'url': 'https://sandersonjames.co.uk/index.php?option=com_propertylab&layout=propertysearch&task=propertysearch&start=0&perpage=20&minprice=1&type=&type=tolet&address3=&prop_type=House+-+Terraced&minbeds=0&minprice=&maxprice=',
                'property_type': 'house'
            },
            {
                'url': 'https://sandersonjames.co.uk/index.php?option=com_propertylab&layout=propertysearch&task=propertysearch&start=0&perpage=20&minprice=1&type=&type=tolet&address3=&prop_type=House+-+Detached&minbeds=0&minprice=&maxprice=',
                'property_type': 'house'
            },
            {
                'url': 'https://sandersonjames.co.uk/index.php?option=com_propertylab&layout=propertysearch&task=propertysearch&start=0&perpage=20&minprice=1&type=&type=tolet&address3=&prop_type=House+-+Semi-Detached&minbeds=0&minprice=&maxprice=',
                'property_type': 'house'
            }]
        
        for item in start_url:
            yield scrapy.Request(url=item['url'],
                                 callback=self.parse,
                                 meta={'request_url': item['url'],
                                       'property_type': item['property_type']})

    def parse(self, response, **kwargs):

        listings = response.xpath('//div[@class="col-sm-12"]')
        for property_item in listings:

            # to avoid let agreed properties and avoid false addition of properties for sale
            let_agreed = property_item.xpath('.//strong[contains(text(),"Let Agreed")]').extract_first()
            per_month = property_item.xpath('.//div[@class="listing-property-price"]/text()').extract_first().lower()
            if let_agreed or 'per month' not in per_month:
                continue

            url = property_item.xpath('.//a[contains(text(),"View Details")]/@href').extract_first()
            url = response.urljoin(url)
            yield scrapy.Request(
                url=url,
                callback=self.get_property_details,
                meta={'request_url': url,
                      'room_count': response.xpath('.//span[@class="beds-curr"]/text()').extract_first(),
                      'property_type': response.meta.get('property_type')})
        
        if len(listings) == 15:
            next_page_url = response.xpath('//a[contains(text()," > ")]/@href').extract_first()
            yield scrapy.Request(
                    url=response.urljoin(next_page_url),
                    callback=self.parse,
                    meta={'request_url': response.urljoin(next_page_url),
                          'property_type': response.meta.get("property_type")})

    def get_property_details(self, response):

        title = response.xpath('//title/text()').extract_first()
        address = response.xpath('//header/h2/text()').extract_first()

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Sandersonjames_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('request_url').split("&id=")[1].split("&")[0])
        item_loader.add_value('room_count', response.meta.get('room_count'))
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('title', title)
        item_loader.add_value('address', address)
        if address:
            item_loader.add_value('city', address.split(', ')[-1].split(' ,')[0])

        # rent = extract_number_only(response.xpath('//span[@class="price_text"]/text()').extract_first())
        item_loader.add_xpath('rent_string', '//span[@class="price_text"]/text()')
        item_loader.add_xpath('images', '//div[@class="item"]//img/@src')
        item_loader.add_xpath('description', '//div[@class="single-property-content"]/text()')
        item_loader.add_xpath('floor_plan_images', '//span[contains(text(),"View Floorplan")]/../@href')

        javascript = response.xpath('//script[contains(text(),"LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latlong = xml_selector.xpath('.//var[@name="myLatlng"]//number/@value').extract()
            item_loader.add_value('latitude', latlong[0])
            item_loader.add_value('longitude', latlong[1])

        features = ", ".join(response.xpath('//div[@class="single-property-content"]//li/text()').extract())

        # https://sandersonjames.co.uk/index.php?option=com_propertylab&task=showproperty&id=819&perpage=5&start=0&Itemid=124
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)
        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)

        # https://sandersonjames.co.uk/index.php?option=com_propertylab&task=showproperty&id=843&perpage=5&start=0&Itemid=124
        if "parking" in features.lower():
            item_loader.add_value('parking', True)
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        # https://sandersonjames.co.uk/index.php?option=com_propertylab&task=showproperty&id=831&perpage=5&start=0&Itemid=124
        if "furnished" in features.lower():
            if "unfurnished" in features.lower():
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        if not item_loader.get_collected_values("furnished"):
            if response.xpath("//li[contains(.,'Furnished')]").get(): item_loader.add_value('furnished', True)

        item_loader.add_value('landlord_name', 'Sanderson James')
        item_loader.add_value('landlord_phone', '0161 231 9696')
        item_loader.add_value('landlord_email', 'lettings@sandersonjames.co.uk')

        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
