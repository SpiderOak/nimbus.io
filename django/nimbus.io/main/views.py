from django.core.urlresolvers import reverse
from django.template.context import Context
from django.http import HttpResponse
from django.conf import settings

LOOKUP = TemplateLookup(directories=['templates'],
                        input_encoding='utf-8',
                        output_encoding='utf-8',
                        encoding_errors='replace')

def render_to_response(request, template, lookup, data=None, context=None):
    template = lookup.get_template(template)
    if data is None:
        data = {}
    if context is None:
        context = Context(data)
    else:
        context.update(data)
    tmpl_data = {}
    for d in context:
        tmpl_data.update(d)
    tmpl_data['reverse'] = reverse

    try:
        return HttpResponse(template.render(**tmpl_data))
    except:
        if settings.DEBUG:
            return HttpResponse(exceptions.html_error_template().render())
        raise

def index(request):
    pass
