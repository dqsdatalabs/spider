# -*- coding: utf-8 -*-
# Author: Noor

import scrapy

from ..helper import sq_feet_to_meters
from ..loaders import ListingLoader

class MySpider(scrapy.Spider):
    name = 'suezhangteam_com'
    allowed_domains = ['suezhangteam.com']
    start_urls = ['https://suezhangteam.com/rent?Type=forRent&view=Gallery&limit=40&p=detail']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'

    def parse(self, response):
        lnks =response.css('#listingslist_1 a ::attr(href)').extract()
        links=[]
        for l in lnks:
            if l!='javascript:;'and l!='':
                links.append(l)
        links=list(dict.fromkeys(links))
        for link in links:
            yield scrapy.Request(
                url='https://suezhangteam.com/'+link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        info=response.css('.listInfo ::text').extract()
        stripped_details = [i.strip().lower() for i in info]
        if 'Commercial' not in info:
            item_loader.add_value('external_link', response.url)
            rent = response.css('.hPrice span ::text').extract()[0].strip()[1:-6]
            item_loader.add_value('rent_string', rent)
            title = response.css('.listAdd::text').extract()[0].strip()
            item_loader.add_value('title', title)
            address=title
            item_loader.add_value('address',address)
            item_loader.add_value('currency', 'CAD')
            item_loader.add_value('external_source', self.external_source)
            id=response.css('title::text').extract()[0].split('|')[0].strip()
            item_loader.add_value('external_id',id)
            images=[]
            for i in range(1,7):
                images.append(f'https://suezhangteam.com/shared/mlphotos/{id.lower()[0]}/{id.lower()}/{id.lower()}_{str(i)}.jpg')
            item_loader.add_value('images', images)
            desc = ''.join(response.css('p+ p::text').extract()[2:-1])
            item_loader.add_value('description', desc)
            item_loader.add_value('landlord_name', 'Sue Zhang')
            item_loader.add_value('landlord_phone', '(905) 305-0505')
            item_loader.add_value('landlord_email', 'sue.realtor@smartsoldrealty.ca')
            item_loader.add_value('property_type', 'apartment')

            if 'size:' in stripped_details:
                i = stripped_details.index('size:')
                value = stripped_details[i + 1]
                feet = value[:value.index('-')]
                item_loader.add_value('square_meters', int(int(sq_feet_to_meters(feet))*10.764))
            if 'beds:' in stripped_details:
                i = stripped_details.index('beds:')
                value = stripped_details[i + 1][0]
                item_loader.add_value('room_count', int(value))
            if 'bath:' in stripped_details:
                i = stripped_details.index('bath:')
                value = stripped_details[i + 1][0]
                item_loader.add_value('bathroom_count', int(value))
            if 'garage:' in stripped_details:
                    item_loader.add_value('parking', True)
            if 'city:'in stripped_details:
                i = stripped_details.index('city:')
                value = stripped_details[i + 1]
                item_loader.add_value('city',value)
            if 'basement:' in stripped_details:
                i = stripped_details.index('basement:')
                value = stripped_details[i + 1]
                if value =='unfurnished':
                    item_loader.add_value('furnished',False)
                else:
                    item_loader.add_value('furnished',True)



            features = response.css('p::text').extract()[1:]
            features = [i.strip().lower() for i in features]
            if 'balcony' in features:
                item_loader.add_value('balcony', True)



            yield item_loader.load_item()
