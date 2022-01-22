# Author: Nipun Arora
# Team: Sabertooth

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces
import re


class HadleighCoUkSpider(scrapy.Spider):
    name = "hadleigh_co_uk"
    allowed_domains = ["hadleigh.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):

        start_urls = ["https://www.hadleigh.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&minprice=&maxprice="]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath('//a[contains(text()," More Details ")]/@href').extract()
        for url in listings:
            yield scrapy.Request(
                url=response.urljoin(url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(url)})
                
        next_page_url = response.xpath('//a[contains(text(),"Next Page")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                    url = response.urljoin(next_page_url),
                    callback = self.parse,
                    meta = {'request_url': response.urljoin(next_page_url)})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//img[contains(@alt,'{$availability}')]//@src").get()
        if status:
            return

        features = ", ".join(response.xpath('//ul[@class="propertyLinks-bullets"]//li//text()').extract())
        
        description = response.xpath('//h2[contains(text(),"Property Description")]/..//p/text()').extract_first()

        houses = ['terrace property', "house", "student property", "villa ", "cottage"]
        apartments = ['flat', 'apartment']
        student_apartments = ['student suite']

        property_type = None
        if "studio" in description.lower():
            property_type = "studio"
        elif any(apartment in description.lower() for apartment in apartments):
            property_type = "apartment"
        elif any(house in description.lower() for house in houses):
            property_type = "house"
        elif any(student_apartment in description.lower() for student_apartment in student_apartments):
            property_type = "student_apartment"
        if property_type:
            item_loader.add_value('property_type', property_type)

        external_link = response.meta.get('request_url')
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split('/')[4])

        # rent
        rent = extract_number_only(response.xpath('//span[@itemprop="price"]/text()').extract_first(), thousand_separator=',', scale_separator='.')
        item_loader.add_value('rent_string', '£' + rent)

        # square_meters
        square_feet = response.xpath('//li[contains(text(), "Sq. Ft")]/text()').extract_first()
        if square_feet:
            square_feet = extract_number_only(square_feet, thousand_separator=',', scale_separator='.')
            square_meters = float(square_feet)*0.092903
            item_loader.add_value('square_meters', square_meters)

        bed_bath = response.xpath('//span[@class="bed-font2"]/text()').extract()
        if bed_bath:
            if extract_number_only(bed_bath[0]) == '0' and property_type == 'studio':
                item_loader.add_value('room_count', '1')
            else:
                item_loader.add_value('room_count', bed_bath[0])

            item_loader.add_value('bathroom_count', bed_bath[-2])

        item_loader.add_xpath('title', './/head//title/text()')

        # address
        address = remove_white_spaces("".join(response.xpath('//div[@class="row address-bar"]//span[@itemprop="name"]//text()').extract()))
        item_loader.add_value('address', address)

        if len(external_link.split('/')) > 6 and external_link.split('/')[6].isalpha():
            item_loader.add_value('city', external_link.split('/')[6])
        item_loader.add_xpath('zipcode', '//div[@class="row address-bar"]//span[@class="red"]//text()')

        # latitude longitude
        lat_long_map = response.xpath('//script[contains(text(),"ShowMap")]/text()').extract_first()
        if lat_long_map:
            lat_long_map = lat_long_map[lat_long_map.find('&q=')+3:].split('"')[0].split('%2C')
            item_loader.add_value('latitude', lat_long_map[0])
            item_loader.add_value('longitude', lat_long_map[-1])

        # images
        images = [response.urljoin(img) for img in response.xpath('//img[@itemprop = "image"]/@src').extract()]
        item_loader.add_value('images', images)

        # floor_plan_images
        floor_plan_images = [response.urljoin(img) for img in response.xpath('//div[@id="floorplanModal"]//img/@src').extract()]
        item_loader.add_value('floor_plan_images', floor_plan_images)

        item_loader.add_xpath('description', './/h2[contains(text(),"Property Description")]/..//p/text()')

        item_loader.add_value('landlord_name', 'Hadleigh Residential')
        item_loader.add_value('landlord_phone', '020 7722 9799')
        item_loader.add_value('landlord_email', 'property@hadleigh.co.uk')

        # https://www.hadleigh.co.uk/property-details/102601000898/greater-london/london/belsize-grove-25?page=1&showstc=on&showsold=on&instruction_type=Letting&minprice=&maxprice=#.X8YlExbhUdU
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        # https://www.hadleigh.co.uk/property-details/102601000350/greater-london/london/ormonde-court?page=1&showstc=on&showsold=on&instruction_type=Letting&minprice=&maxprice=#.X8YlTRbhUdU
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)
            
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)

        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)
        
        # https://www.hadleigh.co.uk/property-details/102601005484/greater-london/london/lambolle-road-2?page=3&n=10&showstc=on&showsold=on&orderby=price+desc&instruction_type=Letting#.X8YlrhbhUdU
        if "parking" in features.lower():
            item_loader.add_value('parking', True)
        
        # https://www.hadleigh.co.uk/property-details/102601005255/greater-london/london/belsize-grove-13?page=1&showstc=on&showsold=on&instruction_type=Letting&minprice=&maxprice=#.X8Yk3RbhUdU
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        # https://www.hadleigh.co.uk/property-details/102601000350/greater-london/london/ormonde-court?page=1&showstc=on&showsold=on&instruction_type=Letting&minprice=&maxprice=#.X8YkqhbhUdU
        if "furnished" in features.lower():
            if "unfurnished" in features.lower():
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Hadleigh_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
