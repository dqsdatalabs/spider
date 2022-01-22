# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
import re
from datetime import datetime

class AllanFullerCoUk(scrapy.Spider):

    name = "allanfuller_co_uk"
    allowed_domains = ["allanfuller.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    api_url = "http://www.allanfuller.co.uk/api/set/results/grid"
    params = {
        'sortorder': 'price-desc',
        'RPP': '12',
        'OrganisationId': '49b4d10c-b821-4939-abba-a3eda383d504',
        'WebdadiSubTypeName': 'Rentals',
        'Status': '{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}',
        'includeSoldButton': 'true',
        'page': '1',
        'incsold': 'true'
    }

    def start_requests(self):
        start_urls = ["http://www.allanfuller.co.uk/let/property-to-let"]
        for url in start_urls:
            yield scrapy.FormRequest(url=self.api_url,
            callback=self.parse,method='POST',formdata=self.params,
            meta={'request_url':url})

    def parse(self, response, **kwargs):
        listings = response.xpath('//div[@class="property "]')
        for property_item in listings:
            url = property_item.xpath('.//div[@class="property-status to-let bg-secondary-transparent"]/../..//a[@class="property-description-link"]/@href').extract_first()
            if url and "result" not in url:
                yield scrapy.Request(
                    url = response.urljoin(url),
                    callback = self.get_property_details,
                    meta = {'request_url' : response.urljoin(url)})

        if len(listings)==12:
            self.params['page']=str(int(self.params['page'])+ 1)
            yield scrapy.FormRequest(url=self.api_url,
            callback=self.parse,method='POST',formdata=self.params)

    def get_property_details(self, response):

        external_link = response.meta.get('request_url')
        description = remove_unicode_char("".join(response.xpath('//section[@id="description"]//p//text()').extract()))
        features = "".join(response.xpath('//a[contains(text(),"Features")]/../..//li/text()').extract())
        

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "AllanFuller_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split('/')[4])

        title = response.xpath('//head/title/text()').extract_first()
        item_loader.add_value('title', title)

        room_bath = response.xpath('//li[@class="FeaturedProperty__list-stats-item"]/span/text()').extract()
        if room_bath:
            item_loader.add_value('room_count', room_bath[0])
            item_loader.add_value('bathroom_count', room_bath[1])

        city = response.xpath('//span[@class="city"]/text()').extract_first()
        if city:
            item_loader.add_value('city', city[:-1])

        zipcode = response.xpath('//span[@class="displayPostCode"]/text()').extract_first()
        if zipcode:    
            item_loader.add_value('zipcode', zipcode)

        address = response.xpath('//span[@class="address1"]/text()').extract_first()
        if address:
            item_loader.add_value('address', address + ', ' + city + zipcode)

        rent = extract_number_only(response.xpath('//span[@class="nativecurrencyvalue"]/text()').extract_first(), thousand_separator=',', scale_separator='.')
        if rent:
            item_loader.add_value('rent_string', '£' + rent)

        lat_long = response.xpath('//section[@id="maps"]/@data-cords').extract_first()
        if lat_long:
            lat_long = lat_long.split(', ')
            item_loader.add_value('latitude', extract_number_only(lat_long[0], thousand_separator=',', scale_separator='.'))
            item_loader.add_value('longitude', extract_number_only(lat_long[-1], thousand_separator=',', scale_separator='.'))

        square_meters = extract_number_only(response.xpath('//li[contains(text(), "sqft")]/text()').extract_first(), thousand_separator=',', scale_separator='.')
        if square_meters:
            square_meters = str(float(square_meters)*0.092903)
            item_loader.add_value('square_meters', square_meters)

        houses = ['terrace property',"house","student property","villa "]
        apartments = ['flat', 'apartment']
        student_apartments = ['student suite']

        title = "".join(response.xpath("//h2[@class='color-primary mobile-left']/text()").extract())

        property_type = None
        if "studio" in title.lower():
            property_type = "studio"
        elif any(apartment in title.lower() for apartment in apartments):
            property_type = "apartment"
        elif any(house in title.lower() for house in houses):
            property_type = "house"
        elif any(student_apartment in title.lower() for student_apartment in student_apartments):
            property_type = "student_apartment"
        item_loader.add_value('property_type', property_type)

        item_loader.add_value('description', description)
        item_loader.add_xpath('images', '//section[@id="gallery"]//div[@class="item"]/div/@data-bg')
        item_loader.add_xpath('floor_plan_images', '//div[@class="floorplan-lightbox"]//img/@src')

        months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
        disc = description.lower().split('available')[-1]
        if "now" in disc or "immediately" in disc:
            available_date = str(datetime.now().year) + '-' + str(datetime.now().month) + '-' + str(datetime.now().day)
            item_loader.add_value('available_date',format_date(available_date))
        else:
            monthh = 0
            for i in range(len(months)):
                if months[i] in disc:
                    monthh=i+1
                    break
            day = str(extract_number_only(disc))
            if day == '0':
                day = '01'
            if monthh == 11 or monthh == 12:
                available_date = '2020' + '-' + str(monthh) + '-' + day
            else:
                available_date = '2021' + '-' + str(monthh) + '-' + day
            if available_date and monthh!=0:
                item_loader.add_value('available_date', format_date(available_date))

        #http://www.allanfuller.co.uk/property/30065312/sw15/london/putney-hill/flat/3-bedrooms
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        #http://www.allanfuller.co.uk/property/30065312/sw15/london/putney-hill/flat/3-bedrooms
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)
        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)

        #http://www.allanfuller.co.uk/property/30027324/sw18/putney/riverside-quarter/apartment/2-bedrooms
        if "swimming pool" in features.lower() or "swimming pool" in description.lower():
            item_loader.add_value('swimming_pool', True)
        
        #http://www.allanfuller.co.uk/property/30065312/sw15/london/putney-hill/flat/3-bedrooms
        if "parking" in features.lower():
            item_loader.add_value('parking', True)
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        #http://www.allanfuller.co.uk/property/30065312/sw15/london/putney-hill/flat/3-bedrooms
        if "furnished" in features.lower():
            if re.search(r"un[^\w]*furnished", features.lower()):
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        if "furnished" in description.lower():
            if re.search(r"un[^\w]*furnished", description.lower()):
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        item_loader.add_value('landlord_name', 'Allan Fuller')
        item_loader.add_xpath('landlord_phone', '//a[contains(@href,"tel")]/text()')
        item_loader.add_xpath('landlord_email', '//a[@class="color-white"][contains(@href,"mailto")]/text()')
        
        self.position += 1
        item_loader.add_value('position', self.position)

        yield item_loader.load_item()